// Package account prepares native RGW accounts through an existing gateway:
// fresh accounts with their tenant, limits and quotas, whose root users are
// created by rgw.Gateway.CreateUser with rgw.UserConfig.Account.
//
// Create creates an owned account through a *rgw.Gateway in the gateway's
// actual native scope. An operation on an owned account is a method of its
// handle, which records the native metadata lifetime it created and refuses a
// deleted and recreated account. These functions act on a gateway that rgw.Run,
// rgw.Start or rgw.WithGateways started; they start no container.
package account
