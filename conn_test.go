package redshiftdatasqldriver

import (
	"database/sql/driver"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"

	"github.com/stretchr/testify/require"
)

func TestRewriteQuery(t *testing.T) {
	cases := []struct {
		casename    string
		query       string
		paramsCount int
		expected    string
	}{
		{
			casename:    "no params",
			query:       `SELECT * FROM pg_user`,
			paramsCount: 0,
			expected:    `SELECT * FROM pg_user`,
		},
		{
			casename:    "no change",
			query:       `SELECT * FROM pg_user WHERE usename = :name`,
			paramsCount: 1,
			expected:    `SELECT * FROM pg_user WHERE usename = :name`,
		},
		{
			casename:    "? rewrite",
			query:       `SELECT 'hoge?' FROM pg_user WHERE usename = ? AND usesysid > ?`,
			paramsCount: 1,
			expected:    `SELECT 'hoge?' FROM pg_user WHERE usename = :1 AND usesysid > :2`,
		},
		{
			casename:    "$ rewrite",
			query:       `SELECT '3$1$' FROM table WHERE "$column" = $1 AND column1 > $2 AND column2 < $1`,
			paramsCount: 1,
			expected:    `SELECT '3$1$' FROM table WHERE "$column" = :1 AND column1 > :2 AND column2 < :1`,
		},
		{
			casename:    "$ rewrite complex, empty quotes",
			query:       `SELECT CONCAT('group ', '', u.usename, '') AS groupname FROM table WHERE "col1" = $1 AND "col2" = $2`,
			paramsCount: 2,
			expected:    `SELECT CONCAT('group ', '', u.usename, '') AS groupname FROM table WHERE "col1" = :1 AND "col2" = :2`,
		},
		{
			casename:    "$ rewrite complex, escaped quote",
			query:       `SELECT CONCAT('group ', '''', u.usename, '''') AS groupname FROM table WHERE "col1" = $1 AND "col2" = $2`,
			paramsCount: 2,
			expected:    `SELECT CONCAT('group ', '''', u.usename, '''') AS groupname FROM table WHERE "col1" = :1 AND "col2" = :2`,
		},
	}
	for _, c := range cases {
		t.Run(c.casename, func(t *testing.T) {
			actual := rewriteQuery(c.query, c.paramsCount)
			require.Equal(t, c.expected, actual)
		})
	}
}

func Test_convertArgsToParameters(t *testing.T) {
	type args struct {
		args []driver.NamedValue
	}
	tests := []struct {
		name string
		args args
		want []types.SqlParameter
	}{
		{
			name: "empty args",
			args: args{
				[]driver.NamedValue{},
			},
			want: nil,
		},
		{
			name: "single arg",
			args: args{
				[]driver.NamedValue{
					{Name: "param1", Value: "value1"},
				},
			},
			want: []types.SqlParameter{
				{Name: aws.String("param1"), Value: aws.String("value1")},
			},
		},
		{
			name: "multiple args",
			args: args{
				[]driver.NamedValue{
					{Name: "param1", Value: "value1"},
					{Name: "param2", Value: 42},
					{Name: "param3", Value: true},
				},
			},
			want: []types.SqlParameter{
				{Name: aws.String("param1"), Value: aws.String("value1")},
				{Name: aws.String("param2"), Value: aws.String("42")},
				{Name: aws.String("param3"), Value: aws.String("true")},
			},
		},
		{
			name: "nil value",
			args: args{
				[]driver.NamedValue{
					{Name: "param1", Value: nil},
				},
			},
			want: []types.SqlParameter{
				{Name: aws.String("param1"), Value: nil},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertArgsToParameters(tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertArgsToParameters() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
