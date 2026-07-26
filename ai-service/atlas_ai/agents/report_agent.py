"""ReportAgent: assembles the final report from ExplanationAgent's
[data]-tagged sentences plus any retrieved literature
(docs/atlas-implementation-spec.md Phase 8, task 6/8). For each literature
sub-question it calls retrieval.retrieve() and tags every returned document
[literature:doc_id] -- retrieve() is stubbed to return no documents in this
slice (../retrieval/__init__.py), so no [literature:...] claim can actually
appear yet; that's the deliberate, visible boundary between this slice and
the one that wires in real pgvector retrieval.
"""

from __future__ import annotations

from .. import retrieval
from .state import PipelineState


class ReportAgent:
    def run(self, state: PipelineState) -> PipelineState:
        for sub_question in state.sub_questions:
            if sub_question.kind != "literature":
                continue
            state.documents.extend(retrieval.retrieve(sub_question.text, state.corpus_id))

        lines = [f"# Research report: {state.question}", ""]
        lines.extend(state.explanation_sentences)
        if state.documents:
            lines.append("")
            lines.append("## Related literature")
            for doc in state.documents:
                lines.append(f"- {doc.text} [literature:{doc.doc_id}]")
        state.report = "\n".join(lines)
        return state
