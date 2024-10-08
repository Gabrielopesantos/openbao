// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package command

import (
	"strings"

	"github.com/hashicorp/cli"
)

var _ cli.Command = (*KVMetadataCommand)(nil)

type KVMetadataCommand struct {
	*BaseCommand
}

func (c *KVMetadataCommand) Synopsis() string {
	return "Interact with OpenBao's Key-Value storage"
}

func (c *KVMetadataCommand) Help() string {
	helpText := `
Usage: bao kv metadata <subcommand> [options] [args]

  This command has subcommands for interacting with the metadata endpoint in
  OpenBao's key-value store. Here are some simple examples, and more detailed
  examples are available in the subcommands or the documentation.

  Create or update a metadata entry for a key:

      $ bao kv metadata put -mount=secret -max-versions=5 -delete-version-after=3h25m19s foo

  Get the metadata for a key, this provides information about each existing
  version:

      $ bao kv metadata get -mount=secret foo

  Delete a key and all existing versions:

      $ bao kv metadata delete -mount=secret foo

  The deprecated path-like syntax can also be used, but this should be avoided 
  for KV v2, as the fact that it is not actually the full API path to 
  the secret (secret/metadata/foo) can cause confusion: 
  
      $ bao kv metadata get secret/foo

  Please see the individual subcommand help for detailed usage information.
`

	return strings.TrimSpace(helpText)
}

func (c *KVMetadataCommand) Run(args []string) int {
	return cli.RunResultHelp
}
