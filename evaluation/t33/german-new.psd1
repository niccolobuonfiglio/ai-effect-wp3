# German node, new orchestrator. ExpectedCommit=HEAD is replaced by run_t33.sh
# with the commit actually checked out in RepositoryRoot.
@{
    RepositoryRoot       = '/home/niccolo/University/Thesis/ai-effect-wp3'
    GermanyRoot          = '/home/niccolo/University/Thesis/ai-effect-wp3/use-cases/germany-node'
    EvidenceRoot         = '/home/niccolo/t33-evidence/german-new'
    ExpectedCommit       = 'HEAD'
    OrchestratorUrl      = 'http://127.0.0.1:18000'
    GermanyComposeFile   = '/home/niccolo/University/Thesis/ai-effect-wp3/use-cases/germany-node/docker-compose.yml'
    VillasServiceName    = 'villas-node'
    VillasImage          = ''
}
