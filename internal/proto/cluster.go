package proto

import "fmt"

type EventBody interface {
	eventType() string
}

func (*UserCreatedEvent) eventType() string         { return "user_created" }
func (*UserUpdatedEvent) eventType() string         { return "user_updated" }
func (*UserDeletedEvent) eventType() string         { return "user_deleted" }
func (*MessageCreatedEvent) eventType() string      { return "message_created" }
func (*ChannelSubscribedEvent) eventType() string   { return "channel_subscribed" }
func (*ChannelUnsubscribedEvent) eventType() string { return "channel_unsubscribed" }
func (*UserBlockedEvent) eventType() string         { return "user_blocked" }
func (*UserUnblockedEvent) eventType() string       { return "user_unblocked" }

func EventTypeFromBody(body EventBody) string {
	if body == nil {
		return ""
	}
	return body.eventType()
}

func (e *ReplicatedEvent) GetTypedBody() EventBody {
	if e == nil {
		return nil
	}
	switch body := e.Body.(type) {
	case *ReplicatedEvent_UserCreated:
		return body.UserCreated
	case *ReplicatedEvent_UserUpdated:
		return body.UserUpdated
	case *ReplicatedEvent_UserDeleted:
		return body.UserDeleted
	case *ReplicatedEvent_MessageCreated:
		return body.MessageCreated
	case *ReplicatedEvent_ChannelSubscribed:
		return body.ChannelSubscribed
	case *ReplicatedEvent_ChannelUnsubscribed:
		return body.ChannelUnsubscribed
	case *ReplicatedEvent_UserBlocked:
		return body.UserBlocked
	case *ReplicatedEvent_UserUnblocked:
		return body.UserUnblocked
	default:
		return nil
	}
}

func (e *ReplicatedEvent) SetTypedBody(body EventBody) error {
	if e == nil {
		return fmt.Errorf("replicated event cannot be nil")
	}
	switch typed := body.(type) {
	case nil:
		e.Body = nil
	case *UserCreatedEvent:
		e.Body = &ReplicatedEvent_UserCreated{UserCreated: typed}
	case *UserUpdatedEvent:
		e.Body = &ReplicatedEvent_UserUpdated{UserUpdated: typed}
	case *UserDeletedEvent:
		e.Body = &ReplicatedEvent_UserDeleted{UserDeleted: typed}
	case *MessageCreatedEvent:
		e.Body = &ReplicatedEvent_MessageCreated{MessageCreated: typed}
	case *ChannelSubscribedEvent:
		e.Body = &ReplicatedEvent_ChannelSubscribed{ChannelSubscribed: typed}
	case *ChannelUnsubscribedEvent:
		e.Body = &ReplicatedEvent_ChannelUnsubscribed{ChannelUnsubscribed: typed}
	case *UserBlockedEvent:
		e.Body = &ReplicatedEvent_UserBlocked{UserBlocked: typed}
	case *UserUnblockedEvent:
		e.Body = &ReplicatedEvent_UserUnblocked{UserUnblocked: typed}
	default:
		return fmt.Errorf("unsupported event body %T", body)
	}
	return nil
}
