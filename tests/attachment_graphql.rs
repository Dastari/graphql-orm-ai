use async_graphql::{EmptySubscription, Schema};
use graphql_orm_ai::{AiAttachmentMutationRoot, AiAttachmentQueryRoot};

#[tokio::test]
async fn attachment_roots_fail_closed_without_authentication() {
    let schema = Schema::build(
        AiAttachmentQueryRoot,
        AiAttachmentMutationRoot,
        EmptySubscription,
    )
    .finish();
    let response = schema
        .execute(
            "{ aiAttachments(sessionId: \"00000000-0000-0000-0000-000000000001\") { edges { cursor } } }",
        )
        .await;
    assert_eq!(response.errors.len(), 1);
}

#[test]
fn attachment_roots_follow_the_complete_configured_graphql_case() {
    let sdl = Schema::build(
        AiAttachmentQueryRoot,
        AiAttachmentMutationRoot,
        EmptySubscription,
    )
    .finish()
    .sdl();

    #[cfg(not(feature = "graphql-case-pascal"))]
    {
        assert!(sdl.contains("aiAttachments(sessionId:"));
        assert!(sdl.contains("createAiAttachmentUpload(input:"));
        assert!(sdl.contains("finalizeAiAttachmentUpload(attachmentId:"));
        assert!(!sdl.contains("AiAttachments("));
    }

    #[cfg(feature = "graphql-case-pascal")]
    {
        assert!(sdl.contains("AiAttachments(SessionId:"));
        assert!(sdl.contains("CreateAiAttachmentUpload(Input:"));
        assert!(sdl.contains("FinalizeAiAttachmentUpload(AttachmentId:"));
        assert!(!sdl.contains("aiAttachments("));
    }
}
