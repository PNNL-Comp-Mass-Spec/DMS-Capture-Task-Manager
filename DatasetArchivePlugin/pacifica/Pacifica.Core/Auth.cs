using System;
using System.IO;
using System.IO.IsolatedStorage;
using System.Net;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security;
using System.Text;
using System.Net.Security;

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
			if (LoggedIn != null)
			{
				LoggedIn(new object(), new EventArgs());
			}
		}

		public Auth(Uri location) : this(location, null) { }

		public Auth(Uri location, Uri proxy)
		{
			_location = location;
			_proxy = proxy;
		}

		public string Location
		{
			get
			{
				return _location.AbsoluteUri;
			}
		}

		public static bool SetCookies(HttpWebRequest request)
		{
			if (request == null)
			{
				return false;
			}

			try
			{
				CookieContainer cc = Auth.GetCookies();
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
					BinaryFormatter bf = new BinaryFormatter();
					cc = bf.Deserialize(ccStream) as CookieContainer;
				}
				catch
				{
					cc = null;
				}
				finally
				{
					if (ccStream != null)
					{
						ccStream.Dispose();
					}
				}
			}
			return cc;
		}

		private bool SaveCookies(CookieContainer cookieJar)
		{
			if (cookieJar != null && cookieJar.Count > 0)
			{
				lock (cookieLock)
				{
					Stream ccStream = null;
					try
					{
						ccStream = GetCookiesFile(FileMode.Create);
						BinaryFormatter bf = new BinaryFormatter();
						bf.Serialize(ccStream, cookieJar);
					}
					catch
					{
						return false;
					}
					finally
					{
						if (ccStream != null)
						{
							ccStream.Dispose();
						}
					}
				}
			}
			RaiseLoggedIn();
			return true;
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
		/// <param name="location">A URI to test user credentials against.</param>
		/// <param name="userName">Username</param>
		/// <param name="password">Password</param>
		/// <param name="throwExceptions">Throws / Re-throws exceptions on error if true.</param>
		/// <returns>On success a NetworkCredential instance is returned.  If throwExceptions equals 
		/// true all exceptions will propogate up the stack, otherwise null is returned.</returns>
		public NetworkCredential GetCredential(string userName, SecureString password, bool throwExceptions = true)
		{
			NetworkCredential cred = null;

			try
			{
				Uri uri = _location;
				bool redirected = false;
				CookieContainer cookieJar = new CookieContainer();

				do
				{
					if (uri.Scheme != "https")
					{
						throw new ArgumentException("Authentication URI must use HTTPS", "location");
					}
					HttpWebRequest request = WebRequest.Create(uri) as HttpWebRequest;

					SetProxy(_proxy, request);

					cred = new NetworkCredential(userName, password);
					request.UseDefaultCredentials = false;
					request.AllowAutoRedirect = false;

					CredentialCache cc = new CredentialCache();
					cc.Add(uri, "Basic", cred);
					request.Credentials = cc;

					request.CookieContainer = cookieJar;

					using (HttpWebResponse resp = request.GetResponse() as HttpWebResponse)
					{
						if ((resp.StatusCode & HttpStatusCode.Redirect) == HttpStatusCode.Redirect)
						{
							string redirect = resp.GetResponseHeader("Location");
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
								throw new ApplicationException("https is the only supported scheme in this method.");
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
				CookieContainer cc = GetCookies();
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
			bool result = false;
			try
			{
				CookieContainer cookieJar = new CookieContainer();

				if (GetAuthCookies(out cookieJar))
				{
					SaveCookies(cookieJar);
					result = false;
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
			Uri uri = _location;
			bool redirected;
			Uri finalUri;
			cookieJar = null;

			bool success = GetAuthCookies(uri, ref cookieJar, out redirected, out finalUri);

			return success;
		}

		/// <summary>
		/// Contacts the MyEMSL auth service to obtain cookies for the currently logged in user
		/// </summary>
		/// <param name="cookieJar">Cookie jar</param>
		/// <returns>True if success; false if an error</returns>
		/// <remarks>Any exceptions that occur will need to be handled by the caller</remarks>
		public bool GetAuthCookies(Uri uri, ref CookieContainer cookieJar, out bool redirected, out Uri finalUri)
		{
			
			redirected = false;
			bool success = false;
			cookieJar = new CookieContainer();

			do
			{
				HttpWebRequest request = WebRequest.Create(uri) as HttpWebRequest;
				request.CookieContainer = cookieJar;
				request.UseDefaultCredentials = true;
				request.AllowAutoRedirect = false;

				request.CookieContainer = cookieJar;

				using (HttpWebResponse resp = request.GetResponse() as HttpWebResponse)
				{
					success = true;
					if ((resp.StatusCode & HttpStatusCode.Redirect) == HttpStatusCode.Redirect)
					{
						string redirect = resp.GetResponseHeader("Location");
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

			return success;
		}

		private bool TestCookieLogin()
		{
			HttpWebRequest request = WebRequest.Create(_location) as HttpWebRequest;
			if (!SetCookies(request))
			{
				return false;
			}

			try
			{
				using (HttpWebResponse resp = request.GetResponse() as HttpWebResponse) { }
				return true;
			}
			catch
			{
				return false;
			}
		}

		private bool TestCookie()
		{
			HttpWebRequest request = WebRequest.Create(_location) as HttpWebRequest;
			CookieContainer cc = GetCookies();
			if (cc == null)
			{
				return false;
			}
			request.CookieContainer = GetCookies();
			request.UseDefaultCredentials = false;
			request.AllowAutoRedirect = true;

			try
			{
				HttpWebResponse resp = request.GetResponse() as HttpWebResponse;
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
			ServicePointManager.ServerCertificateValidationCallback = delegate { return true; };

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
				Auth.ClearCookies();

				Console.WriteLine("Working with " + url);

				Auth auth = new Auth(new Uri(url));

				if (auth.LoginRequired)
				{
					Console.Write("Enter user name: ");
					string userName = Console.ReadLine();

					Console.Write("Enter password: ");
					string pass = ReadPassword();

					var spass = new SecureString();
					foreach (char ch in pass)
					{
						spass.AppendChar(ch);
					}

					try
					{
						NetworkCredential cred = auth.GetCredential(userName, spass);
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
					HttpWebRequest request = WebRequest.Create(url) as HttpWebRequest;
					if (Auth.SetCookies(request))
					{
						Console.WriteLine("Testing Cookie at " + url);

						using (HttpWebResponse resp = request.GetResponse() as HttpWebResponse)
						{
							Console.WriteLine("Cookie test successful");
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
			StringBuilder sb = new StringBuilder();
			bool enter = false;
			do
			{
				ConsoleKeyInfo key = Console.ReadKey(true);
				switch (key.Key)
				{
					case ConsoleKey.Enter:
						enter = true;
						break;
					case ConsoleKey.Backspace:
						int index = sb.Length - 1;
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
