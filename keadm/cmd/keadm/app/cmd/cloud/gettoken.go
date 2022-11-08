package cloud

import (
	"context"
	"fmt"
	hubconfig "github.com/kubeedge/kubeedge/cloud/pkg/cloudhub/config"
	"github.com/spf13/cobra"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/kubeedge/kubeedge/common/constants"
	"github.com/kubeedge/kubeedge/keadm/cmd/keadm/app/cmd/common"
	"github.com/kubeedge/kubeedge/keadm/cmd/keadm/app/cmd/util"
)

var (
	gettokenLongDescription = `
"keadm gettoken" command prints the token to use for establishing bidirectional trust between edge nodes and cloudcore.
A token can be used when a edge node is about to join the cluster. With this token the cloudcore then approve the
certificate request.
`
	gettokenExample = `
keadm gettoken --kube-config /root/.kube/config
- kube-config is the absolute path of kubeconfig which used to build secure connectivity between keadm and kube-apiserver
to get the token.
`
)

// NewGettoken gets the token for edge nodes to join the cluster
func NewGettoken() *cobra.Command {
	init := newGettokenOptions()

	cmd := &cobra.Command{
		Use:     "gettoken",
		Short:   "To get the token for edge nodes to join the cluster",
		Long:    gettokenLongDescription,
		Example: gettokenExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			token, err := queryToken(constants.SystemNamespace, common.TokenSecretName, init.Kubeconfig)
			if err != nil {
				fmt.Printf("failed to get token, err is %s\n", err)
				return err
			}
			token = wrapToken(token, init)
			return showToken(token)
		},
	}
	addGettokenFlags(cmd, init)
	return cmd
}

func addGettokenFlags(cmd *cobra.Command, gettokenOptions *common.GettokenOptions) {
	cmd.Flags().StringVar(&gettokenOptions.Kubeconfig, common.KubeConfig, gettokenOptions.Kubeconfig,
		"Use this key to set kube-config path, eg: $HOME/.kube/config")
	cmd.Flags().StringVar(&gettokenOptions.User, "user", "", "user info")
	cmd.Flags().StringVar(&gettokenOptions.Group, "group", "", "group info")
}

func wrapToken(token []byte, options *common.GettokenOptions) []byte {
	if options.User != "" || options.Group != "" {
		return generateToken(options.User, options.Group)
	} else{
		return token
	}
}
func generateToken(user string, group string)[]byte{
	expiresAt := time.Now().Add(time.Hour * hubconfig.Config.CloudHub.TokenRefreshDuration * 2).Unix()

	token := jwt.New(jwt.SigningMethodHS256)

	token.Claims = jwt.MapClaims{
		"exp": expiresAt,
		"user": user,
		"group": group,
	}

	keyPEM := hubconfig.Config.CaKey
	tokenString, err := token.SignedString(keyPEM)
	fmt.Printf("ca key: \nToken: %s\n", keyPEM,tokenString)
	if err !=nil{
		fmt.Printf("generate token err: %v\n", err)
	}
	return []byte(tokenString)
}
// newGettokenOptions return common options
func newGettokenOptions() *common.GettokenOptions {
	opts := &common.GettokenOptions{}
	opts.Kubeconfig = common.DefaultKubeConfig
	return opts
}

// queryToken gets token from k8s
func queryToken(namespace string, name string, kubeConfigPath string) ([]byte, error) {
	client, err := util.KubeClient(kubeConfigPath)
	if err != nil {
		return nil, err
	}
	secret, err := client.CoreV1().Secrets(namespace).Get(context.Background(), name, metaV1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return secret.Data[common.TokenDataName], nil
}

// showToken prints the token
func showToken(data []byte) error {
	_, err := fmt.Printf(string(data))
	if err != nil {
		return err
	}
	return nil
}
