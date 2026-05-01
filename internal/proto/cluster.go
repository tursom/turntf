// Package proto 包含集群协议的protobuf生成代码及辅助工具。
package proto

import "fmt"

// EventBody 是复制事件体的统一接口，每种事件类型实现eventType()返回类型名称。
type EventBody interface {
	eventType() string
}

// 各事件类型实现EventBody接口。
func (*UserCreatedEvent) eventType() string    { return "user_created" }
func (*UserUpdatedEvent) eventType() string    { return "user_updated" }
func (*UserDeletedEvent) eventType() string    { return "user_deleted" }
func (*MessageCreatedEvent) eventType() string { return "message_created" }
func (*UserAttachmentUpsertedEvent) eventType() string {
	return "user_attachment_upserted"
}
func (*UserAttachmentDeletedEvent) eventType() string {
	return "user_attachment_deleted"
}
func (*UserMetadataUpsertedEvent) eventType() string {
	return "user_metadata_upserted"
}
func (*UserMetadataDeletedEvent) eventType() string {
	return "user_metadata_deleted"
}
func (*UserLoginNameUpsertedEvent) eventType() string {
	return "user_login_name_upserted"
}
func (*UserLoginNameDeletedEvent) eventType() string {
	return "user_login_name_deleted"
}

// EventTypeFromBody 从EventBody中提取事件类型字符串。nil安全。
func EventTypeFromBody(body EventBody) string {
	if body == nil {
		return ""
	}
	return body.eventType()
}

// GetTypedBody 将ReplicatedEvent的oneof body解包为类型化的EventBody接口。
// 支持所有10种事件类型。
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
	case *ReplicatedEvent_UserAttachmentUpserted:
		return body.UserAttachmentUpserted
	case *ReplicatedEvent_UserAttachmentDeleted:
		return body.UserAttachmentDeleted
	case *ReplicatedEvent_UserMetadataUpserted:
		return body.UserMetadataUpserted
	case *ReplicatedEvent_UserMetadataDeleted:
		return body.UserMetadataDeleted
	case *ReplicatedEvent_UserLoginNameUpserted:
		return body.UserLoginNameUpserted
	case *ReplicatedEvent_UserLoginNameDeleted:
		return body.UserLoginNameDeleted
	default:
		return nil
	}
}

// SetTypedBody 将类型化的EventBody设置到ReplicatedEvent的oneof body中。
// 传入nil则清除body；不支持的类型返回错误。
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
	case *UserAttachmentUpsertedEvent:
		e.Body = &ReplicatedEvent_UserAttachmentUpserted{UserAttachmentUpserted: typed}
	case *UserAttachmentDeletedEvent:
		e.Body = &ReplicatedEvent_UserAttachmentDeleted{UserAttachmentDeleted: typed}
	case *UserMetadataUpsertedEvent:
		e.Body = &ReplicatedEvent_UserMetadataUpserted{UserMetadataUpserted: typed}
	case *UserMetadataDeletedEvent:
		e.Body = &ReplicatedEvent_UserMetadataDeleted{UserMetadataDeleted: typed}
	case *UserLoginNameUpsertedEvent:
		e.Body = &ReplicatedEvent_UserLoginNameUpserted{UserLoginNameUpserted: typed}
	case *UserLoginNameDeletedEvent:
		e.Body = &ReplicatedEvent_UserLoginNameDeleted{UserLoginNameDeleted: typed}
	default:
		return fmt.Errorf("unsupported event body %T", body)
	}
	return nil
}
