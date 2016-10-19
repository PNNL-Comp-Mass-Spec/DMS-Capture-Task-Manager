using System;
using System.IO;
using System.IO.IsolatedStorage;
using System.Net;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security;
using System.Text;

namespace Pacifica.Core
{
    public class Auth
    {
        private static readonly object cookieLock = new object();
        private readonly Uri _location;
        private readonly Uri _proxy;
        private const string _cookieName = "Pacifica.Core.AuthCookie.txt";

        public static event EventHandler LoggedIn;
        private static void RaiseLoggedIn()
        {
            LoggedIn?.Invoke(new object(), new EventArgs());
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="location"></param>
        public Auth(Uri location) : this(location, null) { }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="location"></param>
        /// <param name="proxy"></param>
        public Auth(Uri location, Uri proxy)
        {
            _location = location;
            _proxy = proxy;
        }

        // Readonly property
        public string Location => _location.AbsoluteUri;

        public static bool SetCookies(HttpWebRequest request)
        {
            if (request == null)
            {
                return false;
            }

            try
            {
                var cc = GetCookies();
                if (cc == null)
                {
                    return false;
                }
                request.CookieContainer = cc;
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static bool ClearCookies()
        {
            lock (cookieLock)
            {
                var isf = GetAuthStorage();
                if (isf == null)
                {
                    return false;
                }

                try
                {
                    if (isf.FileExists(_cookieName))
                    {
                        isf.DeleteFile(_cookieName);
                    }
                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }

        public static CookieContainer GetCookies()
        {
            CookieContainer cc;
            lock (cookieLock)
            {
                Stream ccStream = null;
                try
                {
                    ccStream = GetCookiesFile(FileMode.Open);
                    var bf = new BinaryFormatter();
                    cc = bf.Deserialize(ccStream) as CookieContainer;
                }
                catch
                {
                    cc = null;
                }
                finally
                {
                    ccStream?.Dispose();
                }
            }
            return cc;
        }

        private void SaveCookies(CookieContainer cookieJar)
        {
            if (cookieJar != null && cookieJar.Count > 0)
            {
                lock (cookieLock)
                {
                    Stream ccStream = null;
                    try
                    {
                        ccStream = GetCookiesFile(FileMode.Create);
                        var bf = new BinaryFormatter();
                        bf.Serialize(ccStream, cookieJar);
                    }
                    catch
                    {
                        return;
                    }
                    finally
                    {
                        ccStream?.Dispose();
                    }
                }
            }
            RaiseLoggedIn();
        }

        private static IsolatedStorageFile GetAuthStorage()
        {
            IsolatedStorageFile isf;
            try
            {
                isf = IsolatedStorageFile.GetStore(IsolatedStorageScope.User | IsolatedStorageScope.Assembly | IsolatedStorageScope.Domain, null, null);
            }
            catch
            {
                isf = null;
            }
            return isf;
        }

        private static Stream GetCookiesFile(FileMode mode)
        {
            var isf = GetAuthStorage();
            if (isf == null)
            {
                return null;
            }

            Stream ccStream;
            try
            {
                ccStream = isf.OpenFile(_cookieName, mode);
            }
            catch
            {
                ccStream = null;
            }

            return ccStream;
        }

        /// <summary>
        /// Get a NetworkCredentials instance associated with location.
        /// </summary>
        /// <param name="userName">Username</param>
        /// <param name="password">Password</param>
        /// <param name="throwExceptions">Throws / Re-throws exceptions on error if true.</param>
        /// <returns>On success a NetworkCredential instance is returned.  If throwExceptions equals 
        /// true all exceptions will propogate up the stack, otherwise null is returned.</returns>
        public NetworkCredential GetCredential(string userName, SecureString password, bool throwExceptions = true)
        {
            NetworkCredential cred;

            try
            {
                var uri = _location;
                bool redirected;
                var cookieJar = new CookieContainer();

                do
                {
                    if (uri.Scheme != "https")
                    {
                        throw new Exception("Authentication URI must use HTTPS; see _location");
                    }
                    var request = WebRequest.Create(uri) as HttpWebRequest;

                    if (request == null)
                        throw new InvalidCastException("Could not cast the WebRequest to an HttpWebRequest for " + uri);

                    SetProxy(_proxy, request);

                    cred = new NetworkCredential(userName, password);
                    request.UseDefaultCredentials = false;
                    request.AllowAutoRedirect = false;

                    var cc = new CredentialCache {
                        { uri, "Basic", cred}
                    };

                    request.Credentials = cc;

                    request.CookieContainer = cookieJar;

                    using (var resp = request.GetResponse() as HttpWebResponse)
                    {
                        if (resp != null && (
                            resp.StatusCode == HttpStatusCode.Redirect ||
                            resp.StatusCode == HttpStatusCode.RedirectMethod ||
                            resp.StatusCode == HttpStatusCode.RedirectKeepVerb))
                        {
                            var redirect = resp.GetResponseHeader("Location");
                            if (redirect.StartsWith("https://"))
                            {
                                uri = new Uri(redirect);
                            }
                            else if (redirect.StartsWith("/"))
                            {
                                uri = new Uri("https://" + uri.Host + redirect);
                            }
                            else
                            {
                                throw new ApplicationException("https is the only supported redirect scheme in method GetCredential");
                            }
                            redirected = true;
                            if (resp.Cookies.Count > 0)
                            {
                                cookieJar.Add(resp.Cookies);
                            }
                        }
                        else
                        {
                            redirected = false;
                            SaveCookies(cookieJar);
                        }
                    }
                } while (redirected);
            }
            catch
            {
                if (throwExceptions)
                {
                    throw;
                }
                cred = null;
            }
            return cred;
        }

        public bool LoginRequired
        {
            get
            {
                if (!IsExplicitLoginRequired())
                {
                    //DefaultCredentials worked, so we can move on.
                    //The authentication cookie was saved.
                    return false;
                }

                // DefaultCredentials might not work, so lets try any cookies
                // that might be on the machine.
                if (TestCookieLogin())
                {
                    return false;
                }

                return true;
            }
        }

        public static bool IsLoggedIn
        {
            get
            {
                var cc = GetCookies();
                if (cc != null && cc.Count > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// Determine whether an explicit login will be required
        /// </summary>
        /// <returns></returns>
        private bool IsExplicitLoginRequired()
        {
            var result = false;
            try
            {
                CookieContainer cookieJar;

                if (GetAuthCookies(out cookieJar))
                {
                    SaveCookies(cookieJar);
                }
            }
            catch
            {
                // Error occurred; integrated authentication did not work (Negotiate protocol) 
                // Explicit credentials will be required
                result = true;
            }
            return result;
        }

        public bool GetAuthCookies(out CookieContainer cookieJar)
        {
            var uri = _location;
            bool redirected;
            Uri finalUri;
            cookieJar = null;

            var success = GetAuthCookies(uri, out cookieJar, out redirected, out finalUri);

            return success;
        }

        /// <summary>
        /// Contacts the MyEMSL auth service to obtain cookies for the currently logged in user
        /// </summary>
        /// <param name="uri">URI</param>
        /// <param name="cookieJar">Cookie jar</param>
        /// <param name="redirected">True if redirected (output)</param>
        /// <param name="finalUri">Final URI (output)</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>Any exceptions that occur will need to be handled by the caller</remarks>
        public bool GetAuthCookies(Uri uri, out CookieContainer cookieJar, out bool redirected, out Uri finalUri)
        {

            redirected = false;
            cookieJar = new CookieContainer();

            do
            {
                var request = WebRequest.Create(uri) as HttpWebRequest;
                if (request == null)
                {
                    finalUri = uri;
                    return false;
                }

                request.CookieContainer = cookieJar;
                request.UseDefaultCredentials = true;
                request.AllowAutoRedirect = false;

                request.CookieContainer = cookieJar;

                using (var resp = request.GetResponse() as HttpWebResponse)
                {
                    if (resp == null)
                    {
                        // No response
                        finalUri = uri;
                        return false;
                    }

                    if (resp.StatusCode == HttpStatusCode.Redirect)
                    {
                        var redirect = resp.GetResponseHeader("Location");
                        if (redirect.StartsWith("http"))
                        {
                            uri = new Uri(redirect);
                        }
                        else if (redirect.StartsWith("/"))
                        {
                            uri = new Uri(uri.Scheme + "://" + uri.Host + redirect);
                        }
                        else
                        {
                            throw new ApplicationException("https and http are the only supported schemes in this method.");
                        }

                        redirected = true;
                        if (resp.Cookies.Count > 0)
                        {
                            cookieJar.Add(resp.Cookies);
                        }
                    }
                    else
                    {
                        redirected = false;
                    }
                }


                if (uri.AbsoluteUri.EndsWith("error/nopersonid"))
                {
                    // Current user is not known to MyEMSL (could be a service account or a new user)
                    finalUri = uri;
                    return false;
                }

            } while (redirected);

            finalUri = uri;

            return true;
        }

        private bool TestCookieLogin()
        {
            var request = WebRequest.Create(_location) as HttpWebRequest;
            if (!SetCookies(request))
            {
                return false;
            }

            try
            {
                if (request == null)
                    return false;

                using (request.GetResponse() as HttpWebResponse)
                { }
                return true;
            }
            catch
            {
                return false;
            }
        }

        private bool TestCookie()
        {
            var request = WebRequest.Create(_location) as HttpWebRequest;
            var cc = GetCookies();
            if (cc == null || request == null)
            {
                return false;
            }

            request.CookieContainer = GetCookies();
            request.UseDefaultCredentials = false;
            request.AllowAutoRedirect = true;

            try
            {
                var resp = request.GetResponse() as HttpWebResponse;
                if (resp == null)
                    return false;

                if ((resp.StatusCode & HttpStatusCode.OK) != HttpStatusCode.OK)
                {
                    return true;
                }
            }
            catch
            {
                return false;
            }

            return true;
        }

        private static void SetProxy(Uri proxy, HttpWebRequest request)
        {
            if (proxy != null)
            {
                request.Proxy = new WebProxy(proxy);
            }
        }

        /// <summary>
        /// Unit testing code...
        /// </summary>
        /// <remarks>Might be useful to move to a unit testing framework
        /// like NUnit or MS Test at some point.</remarks>
        private static void Main(string[] args)
        {
            ServicePointManager.ServerCertificateValidationCallback = delegate
            { return true; };

            string[] urls =
            {
                /* Test URLs:
                // "https://myemsl-dev0.emsl.pnl.gov/myemsl/testauth",
                // "https://a9.my.emsl.pnl.gov/myemsl/testauth",
                 */

                // Official URL
                "https://ingest.my.emsl.pnl.gov/myemsl/testauth"
            };

            foreach (var url in urls)
            {
                Console.WriteLine("Clearing cookies.");
                ClearCookies();

                Console.WriteLine("Working with " + url);

                var auth = new Auth(new Uri(url));

                if (auth.LoginRequired)
                {
                    Console.Write("Enter user name: ");
                    var userName = Console.ReadLine();

                    Console.Write("Enter password: ");
                    var pass = ReadPassword();

                    var spass = new SecureString();
                    foreach (var ch in pass)
                    {
                        spass.AppendChar(ch);
                    }

                    try
                    {
                        var cred = auth.GetCredential(userName, spass);
                        if (cred != null)
                        {
                            Console.WriteLine("Authentication successful");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                else
                {
                    Console.WriteLine("Login not required.");
                }

                try
                {
                    var request = WebRequest.Create(url) as HttpWebRequest;
                    if (SetCookies(request))
                    {
                        Console.WriteLine("Testing Cookie at " + url);

                        if (request == null)
                        {
                            Console.WriteLine("Request is null");
                        }
                        else
                        {
                            using (request.GetResponse() as HttpWebResponse)
                            {
                                Console.WriteLine("Cookie test successful");
                            }
                        }
                    }
                    else
                    {
                        Console.WriteLine("Cookie set returned false.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Cookie test failed.");
                    Console.WriteLine(ex.Message);
                }

            }

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        /// <summary>
        /// Unit testing code.
        /// </summary>
        /// <returns>Password typed on console</returns>
        /// <remarks>Might be useful to move to a unit testing framework
        /// like NUnit or MS Test at some point.</remarks>
        private static string ReadPassword()
        {
            var sb = new StringBuilder();
            var enter = false;
            do
            {
                var key = Console.ReadKey(true);
                switch (key.Key)
                {
                    case ConsoleKey.Enter:
                        enter = true;
                        break;
                    case ConsoleKey.Backspace:
                        var index = sb.Length - 1;
                        if (index > 0)
                        {
                            sb.Remove(index, 1);
                        }
                        break;
                    default:
                        sb.Append(key.KeyChar);
                        break;
                }
            } while (!enter);
            Console.WriteLine(string.Empty);
            return sb.ToString();
        }
    }
}
