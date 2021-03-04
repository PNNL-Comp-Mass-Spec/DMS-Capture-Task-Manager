using PRISM;

namespace CaptureTaskManager
{
    internal class CommandLineOptions
    {
        [Option("T", "CodeTest", "Test", HelpShowsDefault = false, HelpText = "Start the program in code test mode")]
        public bool CodeTestMode { get; set; }

        [Option("Trace", "Verbose", HelpShowsDefault = false, HelpText = "Enable trace mode, where debug messages are written to the command prompt")]
        public bool TraceMode { get; set; }

        [Option("V", "Version", HelpShowsDefault = false, HelpText = "See the program version and OS version")]
        public bool ShowVersionOnly { get; set; }
    }
}
