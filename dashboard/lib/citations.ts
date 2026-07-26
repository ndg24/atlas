// Research reports (POST /research) tag claims inline: "...text [data]" or
// "...text [literature:doc_id]" -- there's no structured field for this, so
// parse the tags out and attach each to the clause immediately preceding it
// (the text since the previous tag, or the start of the report).

export type CitationSegment =
  | { type: "plain"; text: string }
  | { type: "data"; text: string }
  | { type: "literature"; text: string; docId: string };

const TAG_RE = /\[data\]|\[literature:([^\]]+)\]/g;

export function parseCitations(text: string): CitationSegment[] {
  const segments: CitationSegment[] = [];
  let lastIndex = 0;
  let match: RegExpExecArray | null;

  TAG_RE.lastIndex = 0;
  while ((match = TAG_RE.exec(text))) {
    const clause = text.slice(lastIndex, match.index).trim();
    if (match[0] === "[data]") {
      segments.push({ type: "data", text: clause });
    } else {
      segments.push({ type: "literature", text: clause, docId: match[1] });
    }
    lastIndex = TAG_RE.lastIndex;
  }

  const rest = text.slice(lastIndex).trim();
  if (rest) segments.push({ type: "plain", text: rest });

  return segments;
}
