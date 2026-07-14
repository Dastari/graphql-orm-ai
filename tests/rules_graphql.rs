use async_graphql::{EmptySubscription, Schema};
use graphql_orm_ai::{AiRuleMutationRoot, AiRuleQueryRoot};

#[tokio::test]
async fn rule_roots_fail_closed_without_authentication() {
    let schema = Schema::build(AiRuleQueryRoot, AiRuleMutationRoot, EmptySubscription).finish();
    let response = schema
        .execute("{ aiRulePolicy(scope: { kind: \"project\", id: \"one\" }) { rowVersion } }")
        .await;
    assert_eq!(response.errors.len(), 1);
}
