use async_graphql::{EmptySubscription, Schema};
use graphql_orm_ai::{
    AiConfigurationMutationRoot, AiConfigurationQueryRoot, AiMutationRoot, AiQueryRoot,
    AiRuleMutationRoot, AiRuleQueryRoot, AiSkillMutationRoot, AiSkillQueryRoot, AiSubscriptionRoot,
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
    let skill_sdl = Schema::build(AiSkillQueryRoot, AiSkillMutationRoot, EmptySubscription)
        .finish()
        .sdl();
    let rule_sdl = Schema::build(AiRuleQueryRoot, AiRuleMutationRoot, EmptySubscription)
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
        assert!(configuration_sdl.contains("openaiCompatible: AiOpenAiCompatibleProfileInput"));
        assert!(configuration_sdl.contains("providerRetainedContinuation: Boolean!"));
        assert!(skill_sdl.contains("aiSkills(scope:"));
        assert!(skill_sdl.contains("upsertAiSkill(input:"));
        assert!(skill_sdl.contains("publishAiSkillVersion(input:"));
        assert!(skill_sdl.contains("allowedUiIntents: [AiSkillUiIntentBindingInput!]!"));
        assert!(rule_sdl.contains("aiRulePolicy(scope:"));
        assert!(rule_sdl.contains("setAiRulePolicy(input:"));
        assert!(rule_sdl.contains("allowedProviderCapabilities: [AiRuleProviderCapability!]"));
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
        assert!(configuration_sdl.contains("OpenaiCompatible: AiOpenAiCompatibleProfileInput"));
        assert!(configuration_sdl.contains("ProviderRetainedContinuation: Boolean!"));
        assert!(skill_sdl.contains("AiSkills(Scope:"));
        assert!(skill_sdl.contains("UpsertAiSkill(Input:"));
        assert!(skill_sdl.contains("PublishAiSkillVersion(Input:"));
        assert!(skill_sdl.contains("AllowedUiIntents: [AiSkillUiIntentBindingInput!]!"));
        assert!(rule_sdl.contains("AiRulePolicy(Scope:"));
        assert!(rule_sdl.contains("SetAiRulePolicy(Input:"));
        assert!(rule_sdl.contains("AllowedProviderCapabilities: [AiRuleProviderCapability!]"));
        assert!(!rule_sdl.contains("aiRulePolicy("));
        assert!(!configuration_sdl.contains("LOCAL_HARNESS"));
        assert!(!sdl.contains("aiSessions("));
    }
}
