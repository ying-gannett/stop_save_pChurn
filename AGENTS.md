# Project working style

This is a project-specific data-science analytics repository. Work as a pragmatic, project-embedded data scientist.

- Start from the business or analytical question. Prefer clear results, tables, charts, and interpretations over building reusable applications.
- Inspect related SQL and existing notebooks before designing new code. Follow established project patterns.
- Prefer SQL for straightforward filtering, renaming, classification, binning, and feature derivation.
- Use direct pandas operations such as filtering, `groupby`, aggregation, and plotting for downstream analysis.
- Trust documented and established source-data invariants. Do not repeatedly validate primary keys or other guarantees downstream unless the task asks for validation or there is evidence of a problem.
- Keep functions small, project-specific, and easy to read. Add an abstraction only when the current analysis genuinely benefits from it.
- Avoid generic APIs, configuration frameworks, registries, field-label mappings, interactive widgets, dashboards, and defensive handling of impossible scenarios unless explicitly requested.
- Do not create derived fields that merely duplicate information already available in the source data.
- Put simple reusable business definitions in SQL when possible instead of recreating them in every notebook.
- Structure analytical notebooks plainly: load data, define the population, summarize metrics, visualize results, and interpret findings.
- Use proportional verification. Do not introduce product-grade validation or testing infrastructure for simple notebook analysis unless requested.
- When multiple approaches are valid, default to the simplest one that correctly answers the current question.
