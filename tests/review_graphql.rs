use async_graphql::{EmptySubscription, Schema};
use graphql_orm_ai::{
    AiApprovalMutationRoot, AiApprovalQueryRoot, AiProposalMutationRoot, AiProposalQueryRoot,
};

#[tokio::test]
async fn proposal_and_approval_roots_fail_closed_without_authentication() {
    let proposal_schema = Schema::build(
        AiProposalQueryRoot,
        AiProposalMutationRoot,
        EmptySubscription,
    )
    .finish();
    let proposal = proposal_schema
        .execute("{ aiProposals(sessionId: \"00000000-0000-0000-0000-000000000001\") { edges { cursor } } }")
        .await;
    assert_eq!(proposal.errors.len(), 1);

    let approval_schema = Schema::build(
        AiApprovalQueryRoot,
        AiApprovalMutationRoot,
        EmptySubscription,
    )
    .finish();
    let approval = approval_schema
        .execute("{ aiApprovals(sessionId: \"00000000-0000-0000-0000-000000000001\") { edges { cursor } } }")
        .await;
    assert_eq!(approval.errors.len(), 1);
}

#[test]
fn review_roots_follow_the_complete_configured_graphql_case() {
    let proposal_sdl = Schema::build(
        AiProposalQueryRoot,
        AiProposalMutationRoot,
        EmptySubscription,
    )
    .finish()
    .sdl();
    let approval_sdl = Schema::build(
        AiApprovalQueryRoot,
        AiApprovalMutationRoot,
        EmptySubscription,
    )
    .finish()
    .sdl();

    #[cfg(not(feature = "graphql-case-pascal"))]
    {
        assert!(proposal_sdl.contains("aiProposals(sessionId:"));
        assert!(proposal_sdl.contains("reviewAiProposal(input:"));
        assert!(approval_sdl.contains("aiApprovals(sessionId:"));
        assert!(approval_sdl.contains("decideAiApproval(input:"));
        assert!(!proposal_sdl.contains("AiProposals("));
        assert!(!approval_sdl.contains("AiApprovals("));
    }

    #[cfg(feature = "graphql-case-pascal")]
    {
        assert!(proposal_sdl.contains("AiProposals(SessionId:"));
        assert!(proposal_sdl.contains("ReviewAiProposal(Input:"));
        assert!(approval_sdl.contains("AiApprovals(SessionId:"));
        assert!(approval_sdl.contains("DecideAiApproval(Input:"));
        assert!(!proposal_sdl.contains("aiProposals("));
        assert!(!approval_sdl.contains("aiApprovals("));
    }
}
