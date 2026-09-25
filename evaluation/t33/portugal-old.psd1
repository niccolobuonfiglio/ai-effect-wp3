# Portugal node, old orchestrator. TefServicesRoot must point to the external
# TEF checkout (commit 7ba677f7f0a93b5ed6126e43a0b7b0e30f59d6b3 in the T3.3 closure run).
@{
    RepositoryRoot        = '/home/niccolo/University/Thesis/oldCode/ai-effect-wp3'
    TefServicesRoot       = '/home/niccolo/University/Thesis/tef-services'
    IntegratedOverlayRoot = '/home/niccolo/University/Thesis/oldCode/ai-effect-wp3/use-cases/portugal-node-integrated'
    SidecarOverlayRoot    = '/home/niccolo/University/Thesis/oldCode/ai-effect-wp3/use-cases/portugal-node-sidecar'
    IntegratedWorkspace   = '/home/niccolo/t33-work/pt-integrated-old'
    SidecarWorkspace      = '/home/niccolo/t33-work/pt-sidecar-old'
    EvidenceRoot          = '/home/niccolo/t33-evidence/portugal-old'
    ExpectedCommit        = '486ccc702bb2afca79fb7d31258d62ee0f94dbfc'
    OrchestratorUrl       = 'http://127.0.0.1:18000'
    OrchestratorCompose   = '/home/niccolo/University/Thesis/oldCode/ai-effect-wp3/orchestrator/docker-compose.yml'
}
