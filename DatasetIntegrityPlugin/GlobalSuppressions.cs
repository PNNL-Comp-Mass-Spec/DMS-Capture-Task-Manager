// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Performance", "RCS1197:Optimize StringBuilder.Append/AppendLine call.", Justification = "Optimization not necessary", Scope = "module")]
[assembly: SuppressMessage("Readability", "RCS1192:Unnecessary usage of verbatim string literal.", Justification = "Allowed for readability", Scope = "module")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable name", Scope = "type", Target = "~T:DatasetIntegrityPlugin.PluginMain")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "module")]
