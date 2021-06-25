// ReSharper disable UnusedMember.Global
namespace CaptureTaskManager
{
    public static class Conversion
    {
        /// <summary>
        /// Convert string to boolean; default false if an error
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool CBoolSafe(string value)
        {
            return CBoolSafe(value, false);
        }

        /// <summary>
        /// Convert a string value to a boolean
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public static bool CBoolSafe(string value, bool defaultValue)
        {
            if (string.IsNullOrEmpty(value))
            {
                return defaultValue;
            }

            if (bool.TryParse(value, out var parsedValue))
            {
                return parsedValue;
            }

            return defaultValue;
        }

        /// <summary>
        /// Convert a string value to an integer
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public static int CIntSafe(string value, int defaultValue)
        {
            if (string.IsNullOrEmpty(value))
            {
                return defaultValue;
            }

            if (int.TryParse(value, out var parsedValue))
            {
                return parsedValue;
            }

            return defaultValue;
        }

        /// <summary>
        /// Convert a string value to a float
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public static float CSngSafe(string value, float defaultValue)
        {
            var fValue = defaultValue;

            if (string.IsNullOrEmpty(value))
            {
                return fValue;
            }

            if (float.TryParse(value, out fValue))
            {
                return fValue;
            }

            return fValue;
        }

        /// <summary>
        /// Convert returnCode to an integer
        /// </summary>
        /// <param name="returnCode"></param>
        /// <returns>
        /// If returnCode is blank or '0', returns 0
        /// If returnCode is an integer, returns the integer
        /// Otherwise, returns -1
        /// </returns>
        public static int GetReturnCodeValue(string returnCode)
        {
            if (string.IsNullOrWhiteSpace(returnCode))
            {
                return 0;
            }

            if (int.TryParse(returnCode, out var returnCodeValue))
            {
                return returnCodeValue;
            }

            return -1;
        }

        /// <summary>
        /// Surround a file (or directory) path with double quotes if it contains spaces
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        public static string PossiblyQuotePath(string filePath)
        {
            if (string.IsNullOrEmpty(filePath))
            {
                return string.Empty;
            }

            if (filePath.Contains(" "))
            {
                if (!filePath.StartsWith("\""))
                {
                    filePath = "\"" + filePath;
                }

                if (!filePath.EndsWith("\""))
                {
                    filePath += "\"";
                }
            }

            return filePath;
        }
    }
}