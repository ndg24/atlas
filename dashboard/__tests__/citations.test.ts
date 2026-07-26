import { describe, expect, it } from "vitest";
import { parseCitations } from "@/lib/citations";

describe("parseCitations", () => {
  it("returns a single plain segment when there are no tags", () => {
    expect(parseCitations("No citations here.")).toEqual([{ type: "plain", text: "No citations here." }]);
  });

  it("attaches a [data] tag to its preceding clause", () => {
    const result = parseCitations("Revenue rose 12% in Q2. [data]");
    expect(result).toEqual([{ type: "data", text: "Revenue rose 12% in Q2." }]);
  });

  it("attaches a [literature:doc_id] tag with its doc id", () => {
    const result = parseCitations("Comorbidity count predicts readmission risk. [literature:doc-142]");
    expect(result).toEqual([
      { type: "literature", text: "Comorbidity count predicts readmission risk.", docId: "doc-142" },
    ]);
  });

  it("handles a report mixing data and literature claims, plus trailing plain text", () => {
    const text =
      "Hospital F has the highest readmit rate. [data] Comorbidity count is a known risk factor. [literature:doc-1] More research is ongoing.";
    const result = parseCitations(text);
    expect(result).toEqual([
      { type: "data", text: "Hospital F has the highest readmit rate." },
      { type: "literature", text: "Comorbidity count is a known risk factor.", docId: "doc-1" },
      { type: "plain", text: "More research is ongoing." },
    ]);
  });
});
