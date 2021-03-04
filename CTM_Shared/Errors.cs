using System;
using PRISM;

// ReSharper disable UnusedMember.Global
namespace CaptureTaskManager
{
    public static class Errors
    {
        /// <summary>
        /// Parses the .StackTrace text of the given exception to return a compact description of the current stack
        /// </summary>
        /// <param name="ex"></param>
        /// <returns>String similar to "Stack trace: CodeTest.Test-:-CodeTest.TestException-:-CodeTest.InnerTestException in CodeTest.cs:line 86"</returns>
        /// <remarks></remarks>
        public static string GetExceptionStackTrace(Exception ex)
        {
            return GetExceptionStackTrace(ex, false);
        }

        /// <summary>
        /// Parses the .StackTrace text of the given exception to return a compact description of the current stack
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="useMultiLine">>When true, format the stack trace using newline characters instead of -:-</param>
        /// <returns>String similar to "Stack trace: CodeTest.Test-:-CodeTest.TestException-:-CodeTest.InnerTestException in CodeTest.cs:line 86"</returns>
        /// <remarks></remarks>
        public static string GetExceptionStackTrace(Exception ex, bool useMultiLine)
        {
            if (useMultiLine)
            {
                return StackTraceFormatter.GetExceptionStackTraceMultiLine(ex);
            }

            return StackTraceFormatter.GetExceptionStackTrace(ex);
        }
    }
}