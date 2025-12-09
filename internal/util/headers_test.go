package util

import (
	"reflect"
	"testing"
)

func TestPubReplyBodyTemplate(t *testing.T) {
	type args struct {
		body         string
		vars         *VarState
		initTemplate string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "should use inited template, single var",
			args: args{
				vars:         NewVarState(),
				initTemplate: `{{ SetVar "x" "12345" }}`,
				body:         "id={{$x}}",
			},
			want: "id=12345",
		},
		{
			name: "should use inited template, several vars",
			args: args{
				vars:         NewVarState(),
				initTemplate: `{{ SetVar "x" "12345" }}{{ SetVar "y" "54321" }}`,
				body:         "id={{ $x }},uuid={{ $y }}",
			},
			want: "id=12345,uuid=54321",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := PubReplyBodyTemplate(tt.args.initTemplate, "", 0, tt.args.vars)
			if err != nil {
				t.Errorf("PubReplyBodyTemplate() error = %v", err)
				t.FailNow()
			}
			gotB, err := PubReplyBodyTemplate(tt.args.body, "", 0, tt.args.vars)
			if err != nil {
				t.Errorf("PubReplyBodyTemplate() error = %v", err)
				t.FailNow()
			}
			got := string(gotB)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PubReplyBodyTemplate() got = %v, want %v", got, tt.want)
			}
		})
	}
}
