use async_graphql::Schema;
use graphql_orm_ai::{AiMutationRoot, AiQueryRoot, AiSubscriptionRoot};

#[test]
fn configured_graphql_case_is_coherent_without_aliases() {
    let sdl = Schema::build(AiQueryRoot, AiMutationRoot, AiSubscriptionRoot)
        .finish()
        .sdl();

    #[cfg(not(feature = "graphql-case-pascal"))]
    {
        assert!(sdl.contains("aiSessions("));
        assert!(sdl.contains("aiMessages(sessionId:"));
        assert!(sdl.contains("createAiSession(input:"));
        assert!(sdl.contains("aiSessionEvents(sessionId:"));
        assert!(sdl.contains("aiInboxEventPage("));
        assert!(sdl.contains("aiInboxEvents("));
        assert!(!sdl.contains("AiSessions("));
    }

    #[cfg(feature = "graphql-case-pascal")]
    {
        assert!(sdl.contains("AiSessions("));
        assert!(sdl.contains("AiMessages(SessionId:"));
        assert!(sdl.contains("CreateAiSession(Input:"));
        assert!(sdl.contains("AiSessionEvents(SessionId:"));
        assert!(sdl.contains("AiInboxEventPage("));
        assert!(sdl.contains("AiInboxEvents("));
        assert!(!sdl.contains("aiSessions("));
    }
}
