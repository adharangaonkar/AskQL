"""Generate a local SVG graphic of the AskQL LangGraph workflow."""

from __future__ import annotations

from pathlib import Path

from askQL import BasicSQLAgent


def esc(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def node_style(node_id: str) -> tuple[str, str]:
    if node_id == "__start__":
        return "#ffdfba", "__start__"
    if node_id == "__end__":
        return "#baffc9", "__end__"
    return "#fad7de", node_id


def main() -> None:
    agent = BasicSQLAgent(openai_api_key="sk-test")
    graph = agent.workflow.get_graph()

    # Hand-tuned layout for this workflow.
    pos = {
        "__start__": (70, 140),
        "generate_sql": (230, 140),
        "validate_sql": (390, 140),
        "execute_query": (550, 140),
        "format_results": (710, 75),
        "correct_sql": (710, 205),
        "__end__": (870, 140),
    }

    width, height = 940, 280
    box_w, box_h = 130, 44

    lines: list[str] = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">')
    lines.append('<defs>')
    lines.append('<marker id="arrow" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">')
    lines.append('<polygon points="0 0, 10 3.5, 0 7" fill="#444" />')
    lines.append('</marker>')
    lines.append('</defs>')
    lines.append('<rect x="0" y="0" width="100%" height="100%" fill="white"/>')
    lines.append('<text x="470" y="28" font-family="Arial, sans-serif" font-size="20" text-anchor="middle" fill="#222">AskQL LangGraph Workflow</text>')

    for edge in graph.edges:
        sx, sy = pos[edge.source]
        tx, ty = pos[edge.target]

        sy_mid = sy
        ty_mid = ty
        if edge.source == "execute_query" and edge.target == "format_results":
            sy_mid -= 8
            ty_mid += 8
        elif edge.source == "execute_query" and edge.target == "correct_sql":
            sy_mid += 8
            ty_mid -= 8

        x1 = sx + box_w / 2
        x2 = tx - box_w / 2

        style = "stroke:#444;stroke-width:2;fill:none"
        if edge.conditional:
            style += ";stroke-dasharray:7,5"

        lines.append(
            f'<line x1="{x1}" y1="{sy_mid}" x2="{x2}" y2="{ty_mid}" style="{style}" marker-end="url(#arrow)" />'
        )

        if edge.data:
            lx = (x1 + x2) / 2
            ly = (sy_mid + ty_mid) / 2 - 8
            lines.append(
                f'<text x="{lx}" y="{ly}" font-family="Arial, sans-serif" font-size="12" text-anchor="middle" fill="#333">{esc(str(edge.data))}</text>'
            )

    for node_id in graph.nodes.keys():
        x, y = pos[node_id]
        fill, label = node_style(node_id)
        rx = x - box_w / 2
        ry = y - box_h / 2
        lines.append(
            f'<rect x="{rx}" y="{ry}" width="{box_w}" height="{box_h}" rx="12" ry="12" fill="{fill}" stroke="#333" stroke-width="1.5" />'
        )
        lines.append(
            f'<text x="{x}" y="{y + 5}" font-family="Arial, sans-serif" font-size="13" text-anchor="middle" fill="#222">{esc(label)}</text>'
        )

    lines.append('</svg>')

    out_svg = Path("langgraph_workflow.svg")
    out_svg.write_text("\n".join(lines), encoding="utf-8")

    out_mmd = Path("langgraph_workflow.mmd")
    out_mmd.write_text(graph.draw_mermaid(), encoding="utf-8")

    print(f"Wrote {out_svg}")
    print(f"Wrote {out_mmd}")


if __name__ == "__main__":
    main()
