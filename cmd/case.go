package cmd

import (
	"github.com/crazycs520/testutil/test_case"
	"github.com/spf13/cobra"
)

type CaseTest struct {
	*App
}

func (b *CaseTest) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "case",
		Short:        "run case test",
		Long:         `specify the test case, and run the test case`,
		RunE:         b.RunE,
		SilenceUsage: true,
	}

	writeConflict := test_case.NewWriteConflict(b.cfg)
	cmd.AddCommand(writeConflict.Cmd())
	return cmd
}

func (b *CaseTest) RunE(cmd *cobra.Command, args []string) error {
	return cmd.Help()
}
