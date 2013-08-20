using System;
using System.ComponentModel;
using System.Reflection;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Net;
using System.Security;

namespace Pacifica.Core
{
    public static class ExtensionMethods
    {
        /// <summary>
        /// Convert a DateTime type to a UNIX timpestamp.
        /// </summary>
        /// <param name="dt">A DateTime to be converted.</param>
        /// <returns>Seconds since January 1st, 1970 00:00:00 UTC.</returns>
        /// <remarks>Inputing a DateTime with Kind set to anything other than DateTimeKind.Utc 
        /// will convert the structure to UTC before adjusting to the UNIX epoch.</returns>
        public static ulong ToUnixTime(this DateTime dt)
        {
            if (dt.Kind != DateTimeKind.Utc)
            {
                dt = dt.ToUniversalTime();
            }

            TimeSpan t = dt - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            return (ulong)Math.Round(t.TotalSeconds);
        }

        public static string GetDescription<T>(this object enumerationValue) where T : struct
        {
            Type type = enumerationValue.GetType();
            if (!type.IsEnum)
            {
                throw new ArgumentException("EnumerationValue must be of Enum type", "enumerationValue");
            }

            //Tries to find a DescriptionAttribute for a potential friendly name for the enum
            MemberInfo[] memberInfo = type.GetMember(enumerationValue.ToString());
            if (memberInfo != null && memberInfo.Length > 0)
            {
                object[] attrs = memberInfo[0].GetCustomAttributes(typeof(DescriptionAttribute), false);

                if (attrs != null && attrs.Length > 0)
                {
                    //Pull out the description value
                    return ((DescriptionAttribute)attrs[0]).Description;
                }
            }
            //If we have no description attribute, just return the ToString of the enum
            return enumerationValue.ToString();
        }

        public static T DeepClone<T>(this T a)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(stream, a);
                stream.Position = 0;
                return (T)formatter.Deserialize(stream);
            }
        }

      
    }
}