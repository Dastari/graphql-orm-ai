use async_graphql::{EmptySubscription, Schema};
use graphql_orm_ai::{
    AiConfigurationMutationRoot, AiConfigurationQueryRoot, AiMutationRoot, AiQueryRoot,
    AiSubscriptionRoot,
};

#[test]
fn configured_graphql_case_is_coherent_without_aliases() {
    let sdl = Schema::build(AiQueryRoot, AiMutationRoot, AiSubscriptionRoot)
        .finish()
        .sdl();
    let configuration_sdl = Schema::build(
        AiConfigurationQueryRoot,
        AiConfigurationMutationRoot,
        EmptySubscription,
    )
    .finish()
    .sdl();

    #[cfg(not(feature = "graphql-case-pascal"))]
    {
        assert!(sdl.contains("aiSessions("));
        assert!(sdl.contains("aiMessages(sessionId:"));
        assert!(sdl.contains("contentPurged: Boolean!"));
        assert!(sdl.contains("createAiSession(input:"));
        assert!(sdl.contains("aiSessionEvents(sessionId:"));
        assert!(sdl.contains("aiInboxEventPage("));
        assert!(sdl.contains("aiInboxEvents("));
        assert!(sdl.contains("aiUsage(scope:"));
        assert!(configuration_sdl.contains("LOCAL_HARNESS"));
        assert!(!sdl.contains("AiSessions("));
    }

    #[cfg(feature = "graphql-case-pascal")]
    {
        assert!(sdl.contains("AiSessions("));
        assert!(sdl.contains("AiMessages(SessionId:"));
        assert!(sdl.contains("ContentPurged: Boolean!"));
        assert!(sdl.contains("CreateAiSession(Input:"));
        assert!(sdl.contains("AiSessionEvents(SessionId:"));
        assert!(sdl.contains("AiInboxEventPage("));
        assert!(sdl.contains("AiInboxEvents("));
        assert!(sdl.contains("AiUsage(Scope:"));
        assert!(configuration_sdl.contains("LocalHarness"));
        assert!(!configuration_sdl.contains("LOCAL_HARNESS"));
        assert!(!sdl.contains("aiSessions("));
    }
}
