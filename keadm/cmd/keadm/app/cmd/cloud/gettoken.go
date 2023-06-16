package cloud

import (
	"context"
	"fmt"
	"github.com/golang-jwt/jwt"
	"github.com/kubeedge/kubeedge/common/constants"
	"github.com/kubeedge/kubeedge/keadm/cmd/keadm/app/cmd/common"
	"github.com/kubeedge/kubeedge/keadm/cmd/keadm/app/cmd/util"
	"github.com/spf13/cobra"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"time"
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
			token, cakey, err := queryTokenAndCaKey(constants.SystemNamespace, common.TokenSecretName, init.Kubeconfig)
			if err != nil {
				fmt.Printf("failed to get token, err is %s\n", err)
				return err
			}
			token = wrapToken(token, cakey, init)
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
//默认token consisting of caHash and jwt Token，这里将jwt token的内容进行重写，保留原来的caHash
func wrapToken(token []byte, cakey []byte, options *common.GettokenOptions) []byte {
	if options.User != "" || options.Group != "" {
		caHash :=strings.SplitN(string(token),".", 2)[0]
		caHash =fmt.Sprintf("%s.", caHash)
		return append([]byte(caHash),generateToken(cakey, options.User, options.Group)...)
	} else {
		return token
	}
}
func generateToken(cakey []byte, user string, group string) []byte {
	expiresAt := time.Now().Add(time.Hour * 2).Unix()

	token := jwt.New(jwt.SigningMethodHS256)

	token.Claims = jwt.MapClaims{
		"exp":   expiresAt,
		"user":  user,
		"group": group,
	}

	tokenString, err := token.SignedString(cakey)
	if err != nil {
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

// queryTokenAndCaKey gets token from k8s
func queryTokenAndCaKey(namespace string, name string, kubeConfigPath string) ([]byte, []byte, error) {
	client, err := util.KubeClient(kubeConfigPath)
	if err != nil {
		return nil, nil, err
	}
	secret, err := client.CoreV1().Secrets(namespace).Get(context.Background(), name, metaV1.GetOptions{})
	if err != nil {
		return nil, nil, err
	}
	casecret, err := client.CoreV1().Secrets(namespace).Get(context.Background(), "casecret", metaV1.GetOptions{})
	if err != nil {
		return nil, nil, err
	}
	return secret.Data[common.TokenDataName], casecret.Data["cakeydata"], nil
}

// showToken prints the token
func showToken(data []byte) error {
	_, err := fmt.Printf("%s\n",string(data))
	if err != nil {
		return err
	}
	return nil
}
