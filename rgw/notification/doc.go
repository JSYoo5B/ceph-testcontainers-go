// Package notification observes the persistent queue of one RGW bucket
// notification topic through an existing gateway: the topic's native identity
// and delivery limits, its queue statistics and an optional bounded sample of
// the pending entries.
//
// Topics, notification configurations and receivers belong to the SUT and the
// test, which create them through their own S3/SNS client. Queue only reads:
// it never creates, changes, purges or removes a topic, and observing a topic
// does not adopt it. It acts on a gateway that rgw.Run, rgw.Start or
// rgw.WithGateways started; it starts no container.
package notification
