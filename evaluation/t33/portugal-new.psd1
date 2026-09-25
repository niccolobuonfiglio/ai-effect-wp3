# Portugal node, new orchestrator. TefServicesRoot must point to the external
# TEF checkout (commit 7ba677f7f0a93b5ed6126e43a0b7b0e30f59d6b3 in the T3.3 closure run).
# The use-case overlay is taken from OLD for both versions: NEW's copy differs
# since its initial commit, and only the orchestrator may vary.
@{
    RepositoryRoot        = '/home/niccolo/University/Thesis/ai-effect-wp3'
    TefServicesRoot       = '/home/niccolo/University/Thesis/tef-services'
    IntegratedOverlayRoot = '/home/niccolo/University/Thesis/oldCode/ai-effect-wp3/use-cases/portugal-node-integrated'  # same overlay for both versions
    SidecarOverlayRoot    = '/home/niccolo/University/Thesis/oldCode/ai-effect-wp3/use-cases/portugal-node-sidecar'
    IntegratedWorkspace   = '/home/niccolo/t33-work/pt-integrated-new'
    SidecarWorkspace      = '/home/niccolo/t33-work/pt-sidecar-new'
    EvidenceRoot          = '/home/niccolo/t33-evidence/portugal-new'
    ExpectedCommit        = 'HEAD'
    OrchestratorUrl       = 'http://127.0.0.1:18000'
    OrchestratorCompose   = '/home/niccolo/University/Thesis/ai-effect-wp3/orchestrator/docker-compose.yml'
}
