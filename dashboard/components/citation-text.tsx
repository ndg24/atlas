import { parseCitations } from "@/lib/citations";

export function CitationText({ text }: { text: string }) {
  const segments = parseCitations(text);

  return (
    <p className="text-sm leading-relaxed">
      {segments.map((seg, i) => {
        if (seg.type === "plain") return <span key={i}>{seg.text} </span>;
        if (seg.type === "data") {
          return (
            <span key={i}>
              {seg.text}{" "}
              <span className="inline-flex items-center rounded-full bg-blue-100 px-1.5 py-0.5 text-[10px] font-medium text-blue-800 dark:bg-blue-950 dark:text-blue-300">
                data
              </span>{" "}
            </span>
          );
        }
        return (
          <span key={i}>
            {seg.text}{" "}
            <span
              title={seg.docId}
              className="inline-flex items-center rounded-full bg-violet-100 px-1.5 py-0.5 text-[10px] font-medium text-violet-800 dark:bg-violet-950 dark:text-violet-300"
            >
              lit:{seg.docId}
            </span>{" "}
          </span>
        );
      })}
    </p>
  );
}
