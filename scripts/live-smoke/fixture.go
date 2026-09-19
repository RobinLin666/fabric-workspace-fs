package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/transport"

	azruntime "github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/microsoft/fabric-sdk-go/fabric/notebook"
)

type fixtureClient struct {
	http              *transport.Client
	workspace         string
	plan              fixturePlan
	operationID       string
	deleteOperationID string
	returnedID        string
	candidateValid    bool
	ownedID           string
	createSent        bool
	onChange          func() error
	createAdapter     *fixtureSDKTransport
	createPoller      *azruntime.Poller[notebook.ItemsClientCreateNotebookResponse]
}

func (c *fixtureClient) changed() error {
	if c.onChange != nil {
		return c.onChange()
	}
	return nil
}

func fixtureBody(plan fixturePlan) ([]byte, error) {
	body, err := json.Marshal(fixtureRequest(plan))
	if err != nil {
		return nil, fail("cannot encode the new notebook fixture")
	}
	return body, nil
}

func fixtureRequest(plan fixturePlan) notebook.CreateNotebookRequest {
	return notebook.CreateNotebookRequest{
		DisplayName: to.Ptr(plan.NotebookDisplayName), Description: to.Ptr(plan.NotebookDescription),
		Definition: &notebook.Definition{
			Format: to.Ptr("ipynb"),
			Parts: []notebook.DefinitionPart{{
				Path: to.Ptr("notebook-content.ipynb"), PayloadType: to.Ptr(notebook.PayloadTypeInlineBase64),
				Payload: to.Ptr(base64.StdEncoding.EncodeToString(initialNotebook())),
			}},
		},
	}
}

func (c *fixtureClient) create(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	client, adapter, err := c.newSDK(true)
	if err != nil {
		return err
	}
	c.createAdapter = adapter
	if err := c.changed(); err != nil {
		return err
	}
	c.createSent = true
	poller, err := client.BeginCreateNotebook(ctx, c.workspace, fixtureRequest(c.plan), nil)
	if err != nil {
		return sanitizeSDKError(err)
	}
	c.createPoller = poller
	return c.finishSDKCreate(ctx)
}

func (c *fixtureClient) finishSDKCreate(ctx context.Context) error {
	result, err := c.createPoller.PollUntilDone(ctx, &azruntime.PollUntilDoneOptions{Frequency: time.Second})
	if err != nil {
		return sanitizeSDKError(err)
	}
	return c.acceptSDKCreated(ctx, result.Notebook)
}

func (c *fixtureClient) acceptSDKCreated(ctx context.Context, item notebook.Notebook) error {
	if item.ID != nil && fabric.ValidateID(*item.ID) == nil {
		c.returnedID = strings.ToLower(*item.ID)
	}
	if item.ID == nil || item.DisplayName == nil || item.Type == nil {
		return errors.Join(fail("SDK create result omitted required notebook identity fields"), c.changed())
	}
	if item.WorkspaceID != nil && !strings.EqualFold(*item.WorkspaceID, c.workspace) {
		return errors.Join(fail("SDK create result referenced a different workspace"), c.changed())
	}
	value := fabric.Item{ID: *item.ID, DisplayName: *item.DisplayName, Type: string(*item.Type)}
	if item.Description != nil {
		value.Description = *item.Description
	}
	return c.acceptCreated(ctx, value)
}

func waitRetryAfter(ctx context.Context, header string) error {
	delay := time.Second
	if header != "" {
		parsed, err := transport.RetryAfter(header, time.Now())
		if err != nil {
			return fail("service returned an invalid Retry-After header")
		}
		delay = max(delay, parsed)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func (c *fixtureClient) validateIdentity(item fabric.Item, id string, requireMarker bool) error {
	if fabric.ValidateID(id) != nil || !strings.EqualFold(item.ID, id) ||
		item.Type != "Notebook" || item.DisplayName != c.plan.NotebookDisplayName {
		return fail("created notebook identity did not match the exact fixture")
	}
	if requireMarker && item.Description != c.plan.NotebookDescription {
		return fail("created notebook ownership description did not match")
	}
	return nil
}

func (c *fixtureClient) item(ctx context.Context, id string) (fabric.Item, error) {
	if fabric.ValidateID(id) != nil {
		return fabric.Item{}, fail("invalid fixture item UUID")
	}
	resp, err := c.http.Request(ctx, http.MethodGet,
		c.http.URL("/v1/workspaces/"+c.workspace+"/items/"+id, nil), nil, nil, true)
	if err != nil {
		return fabric.Item{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fabric.Item{}, fail("unexpected notebook identity response status")
	}
	var item fabric.Item
	if err := decodeSmall(resp.Body, 1<<20, &item); err != nil {
		return fabric.Item{}, err
	}
	return item, nil
}

func (c *fixtureClient) acceptCreated(ctx context.Context, item fabric.Item) error {
	if fabric.ValidateID(item.ID) != nil {
		return fail("notebook create result had no valid item UUID")
	}
	c.returnedID = strings.ToLower(item.ID)
	identityErr := c.validateIdentity(item, c.returnedID, false)
	if identityErr == nil {
		c.candidateValid = true
	}
	if err := errors.Join(identityErr, c.changed()); err != nil {
		return err
	}
	return c.verifyOwnership(ctx)
}

func (c *fixtureClient) verifyOwnership(ctx context.Context) error {
	if !c.candidateValid || fabric.ValidateID(c.returnedID) != nil {
		return fail("notebook ownership was not established from a create result")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for {
		item, err := c.item(ctx, c.returnedID)
		if isNotFound(err) {
			if err := waitRetryAfter(ctx, ""); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if err := c.validateIdentity(item, c.returnedID, true); err != nil {
			return err
		}
		c.ownedID = c.returnedID
		return c.changed()
	}
}

func (c *fixtureClient) reconcile(ctx context.Context) error {
	if c.ownedID != "" {
		return nil
	}
	if c.candidateValid {
		return c.verifyOwnership(ctx)
	}
	if c.createPoller != nil {
		return c.finishSDKCreate(ctx)
	}
	if c.operationID != "" && c.createAdapter != nil {
		item, err := c.createAdapter.reconcileCreate(ctx)
		if err != nil {
			return sanitizeSDKError(err)
		}
		return c.acceptSDKCreated(ctx, item)
	}
	if c.createSent {
		return fail("create outcome is unverified; planned fixture preserved for manual reconciliation")
	}
	return nil
}

func (c *fixtureClient) remove(ctx context.Context) error {
	if c.ownedID == "" {
		return fail("refusing to delete an unverified notebook")
	}
	item, err := c.item(ctx, c.ownedID)
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := c.validateIdentity(item, c.ownedID, true); err != nil {
		return err
	}
	client, _, err := c.newSDK(false)
	if err != nil {
		return err
	}
	_, requestErr := client.DeleteNotebook(ctx, c.workspace, c.ownedID, nil)
	requestErr = sanitizeSDKError(requestErr)
	var safetyFailure *problem
	if errors.As(requestErr, &safetyFailure) {
		return requestErr
	}
	// Even an ambiguous DELETE failure can be resolved by a same-ID 404.
	confirmCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for {
		current, err := c.item(confirmCtx, c.ownedID)
		if isNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := c.validateIdentity(current, c.ownedID, true); err != nil {
			return err
		}
		if requestErr != nil {
			return requestErr
		}
		if err := waitRetryAfter(confirmCtx, ""); err != nil {
			return err
		}
	}
}
