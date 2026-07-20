"""grpc.aio server implementing AIService (proto/ai.proto), port 9092
(docs/atlas-implementation-spec.md §1.4). Entry point: `python -m
atlas_ai.server` (see ai-service/Dockerfile's ENTRYPOINT).
"""

from __future__ import annotations

import asyncio
import logging

import grpc

from .config import Config
from .explain import narrate_result
from .pb import ai_pb2, ai_pb2_grpc
from .plan.planner import NLToPlanError, nl_to_plan
from .providers.litellm_provider import LiteLLMProvider

logger = logging.getLogger(__name__)


class AIServiceImpl(ai_pb2_grpc.AIServiceServicer):
    def __init__(self, config: Config):
        self._provider = LiteLLMProvider(config.llm_provider, config.llm_model)

    async def NLToQuery(self, request: ai_pb2.NLRequest, context: grpc.aio.ServicerContext) -> ai_pb2.NLResponse:
        try:
            plan_json, raw_output = nl_to_plan(request.question, request.schema_json, self._provider)
            return ai_pb2.NLResponse(plan_json=plan_json, raw_llm_output=raw_output)
        except NLToPlanError as exc:
            return ai_pb2.NLResponse(error=str(exc))

    async def Explain(self, request: ai_pb2.ExplainRequest, context: grpc.aio.ServicerContext) -> ai_pb2.ExplainResponse:
        explanation = narrate_result(request.question, request.result_arrow_ipc, self._provider)
        return ai_pb2.ExplainResponse(explanation=explanation)


async def serve() -> None:
    config = Config.from_env()
    server = grpc.aio.server()
    ai_pb2_grpc.add_AIServiceServicer_to_server(AIServiceImpl(config), server)
    server.add_insecure_port(config.listen_addr)
    logger.info(
        "ai-service listening on %s (provider=%s model=%s)",
        config.listen_addr,
        config.llm_provider,
        config.llm_model,
    )
    await server.start()
    await server.wait_for_termination()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve())


if __name__ == "__main__":
    main()
