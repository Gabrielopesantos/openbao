// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package vault

import (
	"strings"

	"github.com/openbao/openbao/sdk/v2/framework"
	"github.com/openbao/openbao/sdk/v2/logical"
)

func upgradePaths(i *IdentityStore) []*framework.Path {
	return []*framework.Path{
		{
			Pattern: "persona$",

			DisplayAttrs: &framework.DisplayAttributes{
				OperationPrefix: "persona",
				OperationVerb:   "create",
			},

			Fields: map[string]*framework.FieldSchema{
				"id": {
					Type:        framework.TypeString,
					Description: "ID of the persona",
				},
				"entity_id": {
					Type:        framework.TypeString,
					Description: "Entity ID to which this persona belongs to",
				},
				"mount_accessor": {
					Type:        framework.TypeString,
					Description: "Mount accessor to which this persona belongs to",
				},
				"name": {
					Type:        framework.TypeString,
					Description: "Name of the persona",
				},
				"metadata": {
					Type: framework.TypeKVPairs,
					Description: `Metadata to be associated with the persona.
In CLI, this parameter can be repeated multiple times, and it all gets merged together.
For example:
bao <command> <path> metadata=key1=value1 metadata=key2=value2
`,
				},
			},
			Operations: map[logical.Operation]framework.OperationHandler{
				logical.UpdateOperation: &framework.PathOperation{
					Callback: i.handleEntityUpdateCommon(),
				},
			},

			HelpSynopsis:    strings.TrimSpace(aliasHelp["alias"][0]),
			HelpDescription: strings.TrimSpace(aliasHelp["alias"][1]),
		},
		{
			Pattern: "persona/id/" + framework.GenericNameRegex("id"),

			DisplayAttrs: &framework.DisplayAttributes{
				OperationPrefix: "persona",
				OperationSuffix: "by-id",
			},

			Fields: map[string]*framework.FieldSchema{
				"id": {
					Type:        framework.TypeString,
					Description: "ID of the persona",
				},
				"entity_id": {
					Type:        framework.TypeString,
					Description: "Entity ID to which this persona should be tied to",
				},
				"mount_accessor": {
					Type:        framework.TypeString,
					Description: "Mount accessor to which this persona belongs to",
				},
				"name": {
					Type:        framework.TypeString,
					Description: "Name of the persona",
				},
				"metadata": {
					Type: framework.TypeKVPairs,
					Description: `Metadata to be associated with the persona.
In CLI, this parameter can be repeated multiple times, and it all gets merged together.
For example:
bao <command> <path> metadata=key1=value1 metadata=key2=value2
`,
				},
			},

			Operations: map[logical.Operation]framework.OperationHandler{
				logical.UpdateOperation: &framework.PathOperation{
					Callback: i.handleEntityUpdateCommon(),
					DisplayAttrs: &framework.DisplayAttributes{
						OperationVerb: "update",
					},
				},
				logical.ReadOperation: &framework.PathOperation{
					Callback: i.pathAliasIDRead(),
					DisplayAttrs: &framework.DisplayAttributes{
						OperationVerb: "read",
					},
				},
				logical.DeleteOperation: &framework.PathOperation{
					Callback: i.pathAliasIDDelete(),
					DisplayAttrs: &framework.DisplayAttributes{
						OperationVerb: "delete",
					},
				},
			},

			HelpSynopsis:    strings.TrimSpace(aliasHelp["alias-id"][0]),
			HelpDescription: strings.TrimSpace(aliasHelp["alias-id"][1]),
		},
		{
			Pattern: "persona/id/?$",

			DisplayAttrs: &framework.DisplayAttributes{
				OperationPrefix: "persona",
				OperationSuffix: "by-id",
			},

			Operations: map[logical.Operation]framework.OperationHandler{
				logical.ListOperation: &framework.PathOperation{
					Callback: i.pathAliasIDList(),
				},
			},

			HelpSynopsis:    strings.TrimSpace(aliasHelp["alias-id-list"][0]),
			HelpDescription: strings.TrimSpace(aliasHelp["alias-id-list"][1]),
		},
		{
			Pattern: "alias$",

			DisplayAttrs: &framework.DisplayAttributes{
				OperationPrefix: "alias",
				OperationVerb:   "create",
			},

			Fields: map[string]*framework.FieldSchema{
				"id": {
					Type:        framework.TypeString,
					Description: "ID of the alias",
				},
				"entity_id": {
					Type:        framework.TypeString,
					Description: "Entity ID to which this alias belongs to. This field is deprecated in favor of 'canonical_id'.",
				},
				"canonical_id": {
					Type:        framework.TypeString,
					Description: "Entity ID to which this alias belongs to",
				},
				"mount_accessor": {
					Type:        framework.TypeString,
					Description: "Mount accessor to which this alias belongs to",
				},
				"name": {
					Type:        framework.TypeString,
					Description: "Name of the alias",
				},
			},
			Operations: map[logical.Operation]framework.OperationHandler{
				logical.UpdateOperation: &framework.PathOperation{
					Callback: i.handleAliasCreateUpdate(),
				},
			},

			HelpSynopsis:    strings.TrimSpace(aliasHelp["alias"][0]),
			HelpDescription: strings.TrimSpace(aliasHelp["alias"][1]),
		},

		{
			Pattern: "alias/id/" + framework.GenericNameRegex("id"),

			DisplayAttrs: &framework.DisplayAttributes{
				OperationPrefix: "alias",
				OperationSuffix: "by-id",
			},

			Fields: map[string]*framework.FieldSchema{
				"id": {
					Type:        framework.TypeString,
					Description: "ID of the alias",
				},
				"entity_id": {
					Type:        framework.TypeString,
					Description: "Entity ID to which this alias should be tied to. This field is deprecated in favor of 'canonical_id'.",
				},
				"canonical_id": {
					Type:        framework.TypeString,
					Description: "Entity ID to which this alias should be tied to",
				},
				"mount_accessor": {
					Type:        framework.TypeString,
					Description: "Mount accessor to which this alias belongs to",
				},
				"name": {
					Type:        framework.TypeString,
					Description: "Name of the alias",
				},
			},

			Operations: map[logical.Operation]framework.OperationHandler{
				logical.UpdateOperation: &framework.PathOperation{
					Callback: i.handleAliasCreateUpdate(),
					DisplayAttrs: &framework.DisplayAttributes{
						OperationVerb: "update",
					},
				},
				logical.ReadOperation: &framework.PathOperation{
					Callback: i.pathAliasIDRead(),
					DisplayAttrs: &framework.DisplayAttributes{
						OperationVerb: "read",
					},
				},
				logical.DeleteOperation: &framework.PathOperation{
					Callback: i.pathAliasIDDelete(),
					DisplayAttrs: &framework.DisplayAttributes{
						OperationVerb: "delete",
					},
				},
			},

			HelpSynopsis:    strings.TrimSpace(aliasHelp["alias-id"][0]),
			HelpDescription: strings.TrimSpace(aliasHelp["alias-id"][1]),
		},
		{
			Pattern: "alias/id/?$",

			DisplayAttrs: &framework.DisplayAttributes{
				OperationPrefix: "alias",
				OperationSuffix: "by-id",
			},

			Operations: map[logical.Operation]framework.OperationHandler{
				logical.ListOperation: &framework.PathOperation{
					Callback: i.pathAliasIDList(),
				},
			},

			HelpSynopsis:    strings.TrimSpace(aliasHelp["alias-id-list"][0]),
			HelpDescription: strings.TrimSpace(aliasHelp["alias-id-list"][1]),
		},
	}
}
