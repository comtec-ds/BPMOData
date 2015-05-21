using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Xml;
using System.Xml.Linq;
using System.Net.Cache;
using System.IO;
using System.Security.Cryptography;
using System.Text.RegularExpressions;


namespace BPMOData
{

    public static class ODBaseCache
    {
        internal static Dictionary<string, Tuple<DateTime,CookieContainer>> cookieCache = null;
        
        public static CookieContainer GetByKey(string key)
        {
            if (cookieCache != null && cookieCache.ContainsKey(key))
            {
                return cookieCache[key].Item2;
            }
            return null;
        }

        public static void DropByKey(string key)
        {
            if (cookieCache != null && cookieCache.ContainsKey(key))
            {
                cookieCache.Remove(key);
            }
        }

        public static CookieContainer GetByKey(string key, string timeshift)
        {
            if (cookieCache != null && cookieCache.ContainsKey(key))
            {
                int days = 0; int hours = 0; int minutes = 0;
                if (timeshift.EndsWith("m"))
                {
                    int.TryParse(timeshift.Substring(0, timeshift.Length - 1), out minutes);
                }
                if (timeshift.EndsWith("h"))
                {
                    int.TryParse(timeshift.Substring(0, timeshift.Length - 1), out hours);
                }
                if (timeshift.EndsWith("d"))
                {
                    int.TryParse(timeshift.Substring(0, timeshift.Length - 1), out days);
                }

                TimeSpan ts = new TimeSpan(days, hours, minutes, 0);

                if (cookieCache[key].Item1 > (DateTime.UtcNow - ts))
                {
                    return cookieCache[key].Item2;
                }

            }
            return null;
        }

        public static void SetByKey(string key, CookieContainer cookie)
        {
            if (cookieCache == null)
            {
                cookieCache = new Dictionary<string, Tuple<DateTime, CookieContainer>>();
            }
            cookieCache[key] = new Tuple<DateTime, CookieContainer>(DateTime.UtcNow, cookie);
        }

    }



    public class ODBase
    {
        private static readonly XNamespace ds = "http://schemas.microsoft.com/ado/2007/08/dataservices";
        private static readonly XNamespace dsmd = "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata";
        private static readonly XNamespace atom = "http://www.w3.org/2005/Atom";

        private string _dataServer;
        public string _dataServiceUrl;
        private string _dataServiceLogin;
        private string _dataServicePassword;
        private int? _dataServiceSolutionId;
        private string _authMethod;
        private string _authVersion;
        private int _timeoutMS;
        private bool _sessionModeReadOnly;
        private bool _forceSession = true;

        private bool _useHttps = false; // временный костыль
        internal CookieContainer _cookieContainer;

        private int _requestsCompleted = 0;
        public int requestsCompeted
        {
            get { return _requestsCompleted; }
        }

        private void countRequest()
        {
            this._requestsCompleted++;
        }
        public List<string> debugMessages = new List<string>();
        public List<string> errorMessages = new List<string>();

        public void SaveCacheByKey(string key)
        {
            ODBaseCache.SetByKey(key, this._cookieContainer);
        }

        public bool hideConsole = false;

        public void ResetErrorMessages()
        {
            this.errorMessages = new List<string>();
        }
               
        public static class CommonIds
        {
            public static string fileTypeFile = "529BC2F8-0EE0-DF11-971B-001D60E938C6".ToLower();
            public static string fileTypeLink = "539BC2F8-0EE0-DF11-971B-001D60E938C6".ToLower();
        }

        public int Timeout
        {
            get
            {
                return this._timeoutMS;
            }
            set
            {
                if (value > 0)
                {
                    this._timeoutMS = value;
                }
            }
        }

        // filters control characters but allows only properly-formed surrogate sequences
        private static Regex _invalidXMLChars = new Regex(
            @"(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]|[\uD800-\uDBFF](?![\uDC00-\uDFFF])|[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F\uFEFF\uFFFE\uFFFF]",
            RegexOptions.Compiled);

        /// <summary>
        /// removes any unusual unicode characters that can't be encoded into XML
        /// </summary>
        public static string RemoveInvalidXMLChars(string text)
        {
            if (string.IsNullOrEmpty(text)) return "";
            return _invalidXMLChars.Replace(text, "");
        }

        public ODBase(string url, string login, string password, int? solutionId = null, string authMethod = "POST", string authVersion = "5.4", int timeout = 60000, CookieContainer cookies = null, bool useReadonlySessionMode=false, bool ignoreSSLCertificateCheck=false)
        {
            this._dataServer = url;
            this._dataServiceSolutionId = solutionId;
            if (authVersion == "5.4" && solutionId == null)
            {
                solutionId = 0;
            }
            this._dataServiceUrl = url + (solutionId != null ? "/" + ((int)solutionId).ToString() : "") + "/ServiceModel/EntityDataService.svc/";
            this._dataServiceLogin = login;
            this._dataServicePassword = password;
            this._authMethod = authMethod;
            this._authVersion = authVersion;
            this._timeoutMS = timeout;
            this._forceSession = true;
            this._sessionModeReadOnly = useReadonlySessionMode;

            if (url.StartsWith("https://"))
            {
                this._useHttps = true;
            }

            if (ignoreSSLCertificateCheck)
            {
                System.Net.ServicePointManager.ServerCertificateValidationCallback = ((sender, cert, chain, errors) => true);
            }

            if (cookies != null)
            {
                this._cookieContainer = cookies;
            }
            else
            {
                TryLogin();
            }
        }

        public bool CheckAuth(string login, string password)
        {
            bool result = false;

            if (this._authMethod == "POST")
            {
                string loginPath = _dataServer + "/ServiceModel/AuthService.svc/Login";

                var request = HttpWebRequest.Create(loginPath) as HttpWebRequest;
                request.Method = "POST";
                request.ContentType = "application/json";
                if (login == this._dataServiceLogin)
                {
                    _cookieContainer = new CookieContainer();
                    request.CookieContainer = _cookieContainer;
                    if (this._forceSession)
                    {
                        request.Headers.Add("ForceUseSession", "true");
                    }
                }
                request.Timeout = this._timeoutMS;

                using (var requestStream = request.GetRequestStream())
                {
                    using (var writer = new StreamWriter(requestStream))
                    {
                        switch (this._authVersion)
                        {
                            case "5.1":
                                {
                                    writer.Write(@"{
										""UserLogin"":""" + login + @""",
										""UserPassword"":""" + password + @""",
										""Language"":""Ru-ru"",
                                        ""TimeZoneOffset"":0
										}");
                                    break;
                                }
                            case "5.4":
                                {
                                    writer.Write(@"{
										""UserName"":""" + login + @""",
										""UserPassword"":""" + password + @""",
										""Language"":""ru-Ru"",
                                        ""TimeZoneOffset"":0
										}");

                                    break;
                                }
                        }
                    }
                }
                try
                {
                    using (var response = (HttpWebResponse)request.GetResponse())
                    {
                        if (response.StatusCode == HttpStatusCode.OK)
                        {
                            result = true;
                        }
                        response.Close();
                    }
                }
                catch (Exception Ex)
                { 

                }
            }
            if (this._authMethod == "GET")
            {
                string loginPath = _dataServer + "/ServiceModel/AuthService.svc/Login?UserName=" + login + "&UserPassword=" + password + "&SolutionName=TSBpm";

                var request = HttpWebRequest.Create(loginPath) as HttpWebRequest;
                request.Method = "GET";
                request.ContentType = "application/json";
                if (login == this._dataServiceLogin)
                {
                    _cookieContainer = new CookieContainer();
                    request.CookieContainer = _cookieContainer;
                    request.Headers.Add("ForceUseSession", "true");
                }
                request.Timeout = this._timeoutMS;

                try
                {
                    using (var response = (HttpWebResponse)request.GetResponse())
                    {
                        if (response.StatusCode == HttpStatusCode.OK)
                        {
                            result = true;
                        }
                        response.Close();
                    }
                }
                catch (Exception Ex)
                {

                }
            }
            return result;
        }

        protected internal bool TryLogin()
        {
            return this.CheckAuth(this._dataServiceLogin, this._dataServicePassword);
        }

        public List<string> GetCollections()
        {
            List<string> collections = new List<string>();
            var request = (HttpWebRequest)HttpWebRequest.Create(this._dataServiceUrl);
            request.Credentials = new NetworkCredential(this._dataServiceLogin, this._dataServicePassword);
            request.Method = "GET";
            if (this._forceSession)
            {
                request.Headers.Add("ForceUseSession", "true");
            }
            if (this._sessionModeReadOnly)
            {
                request.Headers.Add("Bpmonline-Session-Mode", "ReadOnly");
            }
            request.Timeout = this._timeoutMS;

            if (_cookieContainer == null || _cookieContainer.Count == 0)
            {
                this.TryLogin();
            }
            request.CookieContainer = _cookieContainer;

            try
            {
                using (var response = request.GetResponse())
                {
                    XmlDocument xd = new XmlDocument();
                    xd.Load(response.GetResponseStream());

                    if (true || xd.SelectNodes("child::service").Count == 1)
                    {
                        foreach (XmlElement xe in xd["service"]["workspace"].ChildNodes)
                        {
                            if (xe.Name == "collection" && xe.HasAttribute("href"))
                            {
                                string href = xe.GetAttribute("href");
                                if (!collections.Contains(href))
                                {
                                    collections.Add(href);
                                }
                            }
                        }
                    }
                    else
                    {
                        if (!this.hideConsole)
                        {
                            Console.WriteLine("Missing [service] node in resulting xml");
                        }
                        this.errorMessages.Add(DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss") + "Failed to retrieve collections");
                    }
                    response.Close();
                }
            }
            catch (WebException e)
            {
                throw new ODWebException(e);
            }

            this.countRequest();
            return collections;
        }

        public string GetCollectionPageUrl(string collectionName)
        {
            return this._dataServiceUrl + collectionName;
        }


        protected internal List<XmlElement> GetAllPages(string collection, string query, int maxIterations = 10)
        {
            return this.GetAllPages(collection, query, "", maxIterations);
        }

        protected internal List<XmlElement> GetAllPages(string collection, string query, string fields, int maxIterations=10)
        {
            List<XmlElement> result = new List<XmlElement>();

            bool goNext = true;
            string goUrl = this._dataServiceUrl + collection + "Collection"+(fields!=""? "?$select="+fields:"") + (query != "" ? (fields!=""?"&":"?")+ "$filter=" + query : "");
            int iter = 0;
            while (goNext && iter<maxIterations)
            {
                goNext = false;
                List<XmlElement> entries = this.GetPage(goUrl);

                foreach (XmlElement entry in entries)
                {
                    if (entry.Name == "entry")
                    {
                        result.Add(entry);
                    }
                    if (entry.Name == "link" && entry.HasAttribute("rel") && entry.GetAttribute("rel") == "next" && entry.HasAttribute("href"))
                    {
                        string goUrlNext = entry.GetAttribute("href");
                        if (goUrl != goUrlNext)
                        {
                            goNext = true;
                            goUrl = goUrlNext;
                        }
                    }
                }
                iter++;
            }

            return result;
        }

        public int GetCollectionSize(string Collection)
        {
            int result = 0;
            string url = this._dataServiceUrl + Collection + "Collection?$inlinecount=allpages&$select=Id&$top=1";
            if (this._useHttps && url.StartsWith("http://")) // костыль
            {
                url = url.Replace("http://", "https://");
            }

            XmlDocument xd = new XmlDocument();
            var request = (HttpWebRequest)HttpWebRequest.Create(url);
            request.Credentials = new NetworkCredential(this._dataServiceLogin, this._dataServicePassword);
            request.Method = "GET";
            if (this._forceSession)
            {
                request.Headers.Add("ForceUseSession", "true");
            }
            if (this._sessionModeReadOnly)
            {
                request.Headers.Add("Bpmonline-Session-Mode", "ReadOnly");
            }
            request.Timeout = this._timeoutMS;

            if (_cookieContainer == null || _cookieContainer.Count == 0)
            {
                this.TryLogin();
            }
            request.CookieContainer = _cookieContainer;

            try
            {
                using (var response = (HttpWebResponse)request.GetResponse())
                {
                    xd.Load(response.GetResponseStream());
                    if (xd.GetElementsByTagName("feed").Count == 1)
                    {
                        foreach (XmlElement xe in xd["feed"].ChildNodes)
                        {
                            if (xe.Name == "m:count")
                            {
                                int.TryParse(xe.InnerText, out result);
                            }
                        }
                    }
                    response.Close();
                }
            }
            catch (WebException e)
            {
                throw new ODWebException(e);
            }

            return result;
        }

        protected internal byte[] GetData(string url)
        {
            byte[] bytes = new byte[0];
            if (this._useHttps && url.StartsWith("http://")) // костыль
            {
                url = url.Replace("http://", "https://");
            }

            try
            {
                var request = (HttpWebRequest)HttpWebRequest.Create(url);
                request.Credentials = new NetworkCredential(this._dataServiceLogin, this._dataServicePassword);
                request.Method = "GET";
                if (this._forceSession)
                {
                    request.Headers.Add("ForceUseSession", "true");
                }
                if (this._sessionModeReadOnly)
                {
                    request.Headers.Add("Bpmonline-Session-Mode", "ReadOnly");
                }
                request.Timeout = this._timeoutMS;

                if (_cookieContainer == null || _cookieContainer.Count == 0)
                {
                    this.TryLogin();
                }
                request.CookieContainer = _cookieContainer;

                using (var response = (HttpWebResponse)request.GetResponse())
                {
                    Stream s = response.GetResponseStream();
                    using (var ms = new MemoryStream())
                    {
                        s.CopyTo(ms);
                        bytes = ms.ToArray();
                    }
                    s.Close();   
                    response.Close();
                }
            }
            catch (WebException e) 
            {
                if (!this.hideConsole)
                {
                    Console.WriteLine(DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss") + ": Failed to retrieve data @ " + url);
                }
                this.errorMessages.Add(DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss") + "Failed to retrieve data @ " + url);

                throw new ODWebException(e);
            }
            this.countRequest();
            return bytes;
        }

       

        public List<XmlElement> GetPage(string url)
        {
            if (this._useHttps && url.StartsWith("http://")) // костыль
            {
                url = url.Replace("http://", "https://");
            }

            XmlDocument xd = new XmlDocument();
            List<XmlElement> result = new List<XmlElement>();
            
           
            try
            {
                var request = (HttpWebRequest)HttpWebRequest.Create(url);
                request.Credentials = new NetworkCredential(this._dataServiceLogin, this._dataServicePassword);
                request.Method = "GET";
                if (this._forceSession)
                {
                    request.Headers.Add("ForceUseSession", "true");
                }
                if (this._sessionModeReadOnly)
                {
                    request.Headers.Add("Bpmonline-Session-Mode", "ReadOnly");
                }
                request.Timeout = this._timeoutMS;

                if (_cookieContainer == null || _cookieContainer.Count == 0)
                {
                    this.TryLogin();
                }
                request.CookieContainer = _cookieContainer;


                using (var response = (HttpWebResponse)request.GetResponse())
                {
                    xd.Load(response.GetResponseStream());
                    if (xd.GetElementsByTagName("feed").Count == 1)
                    {
                        foreach (XmlElement xe in xd["feed"].ChildNodes)
                        {
                            if (xe.Name == "entry")
                            {
                                result.Add(xe);
                            }
                            if (xe.Name == "link") // признак следующей страницы
                            {
                                result.Add(xe);
                            }
                        }
                    }
                    else
                    {
                        if (xd.GetElementsByTagName("entry").Count == 1)
                        {
                            result.Add(xd["entry"]);
                        }
                    }
                    response.Close();
                }
            }
            catch (XmlException e)
            {
                if (!this.hideConsole)
                {
                    Console.WriteLine(DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss") + ": Error in XML @ " + url);
                }
                this.errorMessages.Add(DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss") + ": Error in XML page @ " + url);
                //throw e;
            }
            catch (WebException e)
            {
                if (!this.hideConsole)
                {
                    Console.WriteLine(DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss") + ": Failed to retrieve page @ " + url);
                }
                this.errorMessages.Add(DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss") + ": Failed to retrieve page @ " + url);

                ODWebException except = new ODWebException(e);
                throw except;
            }

           
            this.countRequest();
            return result;
        }

        
        public static string Dequote(string Name)
        {
            return Name.Replace("'", "''");
        }

        public static string XMLQuotes(string Name)
        {
            if (Name != null)
            {
                return Name.Replace("&", "&amp;").Replace("\"", "&quot;").Replace("<", "&lt;").Replace(">", "&gt;").Replace("\n", " ").Replace("\r", " ");
            }
            else
            {
                return null;
            }
        }
        


        public string AddItem(string Collection, Dictionary<string, object> data)
        {

            List<XElement> xeData = new List<XElement>();
            foreach (KeyValuePair<string, object> kvp in data)
            {
                if (kvp.Value != null)
                {
                    xeData.Add(new XElement(ds + kvp.Key, RemoveInvalidXMLChars(kvp.Value.ToString()) ));
                }
            }

            XElement content = new XElement(dsmd + "properties", xeData);
            XElement entry = new XElement(atom + "entry", new XElement(atom + "content", new XAttribute("type", "application/xml"), content));

            var request = (HttpWebRequest)HttpWebRequest.Create(this._dataServiceUrl + Collection + "Collection/");
            request.Credentials = new NetworkCredential(this._dataServiceLogin, this._dataServicePassword);
            request.Method = "POST";
            request.Accept = "application/atom+xml";
            request.ContentType = "application/atom+xml;type=entry";
            if (this._forceSession)
            {
                request.Headers.Add("ForceUseSession", "true");
            }
            if (this._sessionModeReadOnly)
            {
                request.Headers.Add("Bpmonline-Session-Mode", "ReadOnly");
            }
            request.Timeout = this._timeoutMS;

            if (_cookieContainer == null || _cookieContainer.Count == 0)
            {
                this.TryLogin();
            }
            request.CookieContainer = _cookieContainer;

            string result = "?";
            try
            {
                using (var writer = XmlWriter.Create(request.GetRequestStream()))
                {
                    entry.WriteTo(writer);
                }
            }
            catch(Exception ex)
            {
                WebException we = new WebException("Error in writing XML");
                throw new ODWebException(we);
            }

            try
            {
                WebResponse response = request.GetResponse();
                if (((HttpWebResponse)response).StatusCode == HttpStatusCode.Created)
                {
                    result = response.Headers["Location"];
                }
                response.Close();
            }
            catch (WebException e)
            {
                string details = "";
                try
                {
                    details = new StreamReader(e.Response.GetResponseStream()).ReadToEnd();
                }
                catch(Exception ex) { }
                ODWebException odwe = new ODWebException(e);
                throw odwe;
            }

            this.countRequest();
            return result;
        }

        protected internal static string GetDataLink(XmlElement entry)
        {
            string result = "";
            foreach (XmlElement xe in entry.ChildNodes)
            {
                if (xe.Name == "link" && xe.HasAttribute("title") && xe.GetAttribute("title").ToLower() == "data" && xe.HasAttribute("rel") && xe.GetAttribute("rel").ToLower().EndsWith("edit-media/data") && xe.HasAttribute("href"))
                {
                    result = xe.GetAttribute("href");
                    break;
                }
            }
            return result;
        }

        protected internal static Dictionary<string, object> GetEntryFields(XmlElement entry)
        {
            Dictionary<string, object> val = new Dictionary<string, object>();
            foreach (XmlElement xe in entry.ChildNodes)
            {
                if (xe.Name == "content")
                {
                    foreach (XmlElement d in xe["m:properties"].ChildNodes)
                    {
                        string dName = d.Name.Replace("d:", "");
                        if (d.InnerText != "")
                        {
                            string anotherValue = "";
                            if (d.HasAttribute("m:type")) // пропускаем null-значения разных типов
                            {
                                string attr = d.GetAttribute("m:type");
                                if (attr == "Edm.Guid" && d.InnerText == Guid.Empty.ToString())
                                {
                                    continue;
                                }
                                if (attr == "Edm.DateTime" && (d.InnerText == "0001-01-01T00:00:00" || d.InnerText == DateTime.MinValue.ToString()))
                                {
                                    continue;
                                }
                                if (attr == "Edm.Boolean")
                                {
                                    if (d.InnerText=="false") {anotherValue="0";}
                                    if (d.InnerText=="true") {anotherValue="1";}
                                }
                            }
                            
                            val[dName] = anotherValue=="" ? d.InnerText : anotherValue;
                        }
                    }
                }
                if (xe.Name == "link" && xe["m:inline"]!=null)
                {
                    string linkTitle = xe.GetAttribute("title");
                    foreach (XmlElement xe2 in xe["m:inline"])
                    {
                        if (xe2.Name == "entry")
                        {
                            Dictionary<string, object> val2 = GetEntryFields(xe2);
                            foreach (KeyValuePair<string, object> kvp in val2)
                            {
                                val[linkTitle + "__" + kvp.Key] = kvp.Value;
                            }
                        }
                    }
                }
            }
            return val;
        }

        

        public void DeleteItem(string Collection, string Guid)
        {
            var request = (HttpWebRequest)HttpWebRequest.Create(this._dataServiceUrl + Collection + "Collection(guid'" + Guid + "')/");
            request.Credentials = new NetworkCredential(this._dataServiceLogin, this._dataServicePassword);
            request.Method = "DELETE";
            if (this._forceSession)
            {
                request.Headers.Add("ForceUseSession", "true");
            }
            if (this._sessionModeReadOnly)
            {
                request.Headers.Add("Bpmonline-Session-Mode", "ReadOnly");
            }
            request.Timeout = this._timeoutMS;

            if (_cookieContainer == null || _cookieContainer.Count == 0)
            {
                this.TryLogin();
            }
            request.CookieContainer = _cookieContainer;

            try
            {
                WebResponse response = request.GetResponse();
                response.Close();
            }
            catch (WebException e)
            {
                throw new ODWebException(e);
            }
            
        }

        public void DeleteLink(string Collection, string Guid, string CollectionTo)
        {
            var request = (HttpWebRequest)HttpWebRequest.Create(this._dataServiceUrl + Collection + "Collection(guid'" + Guid + "')/$links/" + CollectionTo);
            request.Credentials = new NetworkCredential(this._dataServiceLogin, this._dataServicePassword);
            request.Method = "DELETE";
            if (this._forceSession)
            {
                request.Headers.Add("ForceUseSession", "true");
            }
            if (this._sessionModeReadOnly)
            {
                request.Headers.Add("Bpmonline-Session-Mode", "ReadOnly");
            }
            request.Timeout = this._timeoutMS;

            if (_cookieContainer == null || _cookieContainer.Count == 0)
            {
                this.TryLogin();
            }
            request.CookieContainer = _cookieContainer;
            
            try
            {
                WebResponse response = request.GetResponse();
                response.Close();
            }
            catch (WebException e)
            {
                throw new ODWebException(e);
            }
        }

        public string UploadBinary(string Collection, string Guid, byte[] bytes, bool returnSHA256=true)
        {
            var request = (HttpWebRequest)HttpWebRequest.Create(this._dataServiceUrl + Collection + "Collection(guid'" + Guid + "')/Data");

            request.Credentials = new NetworkCredential(this._dataServiceLogin, this._dataServicePassword);
            request.Method = "PUT";
            request.Accept = "application/atom+xml";
            request.ContentLength = bytes.Length;
            request.SendChunked = true;
            request.ContentType = "application/octet-stream";
            if (this._forceSession)
            {
                request.Headers.Add("ForceUseSession", "true");
            }
            if (this._sessionModeReadOnly)
            {
                request.Headers.Add("Bpmonline-Session-Mode", "ReadOnly");
            }
            request.Timeout = this._timeoutMS;

            if (_cookieContainer == null || _cookieContainer.Count == 0)
            {
                this.TryLogin();
            }
            request.CookieContainer = _cookieContainer;

            using (Stream stream = request.GetRequestStream())
            {
                stream.Write(bytes, 0, bytes.Length);
            }

            try
            {
                WebResponse response = request.GetResponse();
                response.Close();

                if (returnSHA256)
                {
                    SHA256 sha = SHA256Managed.Create();
                    byte[] hash = sha.ComputeHash(bytes);
                    string result = "";
                    int i;
                    for (i = 0; i < hash.Length; i++)
                    {
                        result += String.Format("{0:X2}", hash[i]);
                    }
                    return result;
                }
                else
                {
                    return "OK";
                }
            }
            catch (WebException e)
            {
                throw new ODWebException(e);
            }
        }

        public string UpdateItem(string Collection, string Guid, Dictionary<string, object> data)
        {

            List<XElement> xeData = new List<XElement>();
            foreach (KeyValuePair<string, object> kvp in data)
            {
                if (kvp.Value != null)
                {
                    xeData.Add(new XElement(ds + kvp.Key, RemoveInvalidXMLChars(kvp.Value.ToString())));
                }
                else
                {
                    XElement ne = new XElement(ds + kvp.Key);
                    ne.SetAttributeValue(dsmd + "null", "true");
                    xeData.Add(ne);
                }
            }

            XElement content = new XElement(dsmd + "properties", xeData);

            XElement entry = new XElement(atom + "entry", new XElement(atom + "content", new XAttribute("type", "application/xml"), content));

            var request =
                    (HttpWebRequest)HttpWebRequest.Create(this._dataServiceUrl + Collection + "Collection(guid'" + Guid + "')/");
            request.Credentials = new NetworkCredential(this._dataServiceLogin, this._dataServicePassword);
            request.Method = "PUT";
            request.Accept = "application/atom+xml";
            request.ContentType = "application/atom+xml;type=entry";
            if (this._forceSession)
            {
                request.Headers.Add("ForceUseSession", "true");
            }
            if (this._sessionModeReadOnly)
            {
                request.Headers.Add("Bpmonline-Session-Mode", "ReadOnly");
            }
            request.Timeout = this._timeoutMS;

            if (_cookieContainer == null || _cookieContainer.Count == 0)
            {
                this.TryLogin();
            }
            request.CookieContainer = _cookieContainer;

            string result = "";
            try
            {
                
                using (var writer = XmlWriter.Create(request.GetRequestStream()))
                {
                    entry.WriteTo(writer);
                }
            }
            catch(Exception ex)
            {
                WebException we = new WebException("Error in writing XML");
                throw new ODWebException(we);
            }

            try
            {
                WebResponse response = request.GetResponse();
                result = "ok";
                response.Close();
            }
            catch (WebException e)
            {
                throw new ODWebException(e);
            }

            this.countRequest();
            return result;
        }




        public XmlDocument GetMetadata()
        {
            XmlDocument xd = new XmlDocument(); ;
            string url = this._dataServiceUrl + "$metadata";
            
            if (this._useHttps && url.StartsWith("http://")) // костыль
            {
                url = url.Replace("http://", "https://");
            }

            try
            {
                var request = (HttpWebRequest)HttpWebRequest.Create(url);
                request.Credentials = new NetworkCredential(this._dataServiceLogin, this._dataServicePassword);
                request.Method = "GET";
                if (this._forceSession)
                {
                    request.Headers.Add("ForceUseSession", "true");
                }
                if (this._sessionModeReadOnly)
                {
                    request.Headers.Add("Bpmonline-Session-Mode", "ReadOnly");
                }
                request.Timeout = this._timeoutMS;

                if (_cookieContainer == null || _cookieContainer.Count == 0)
                {
                    this.TryLogin();
                }
                request.CookieContainer = _cookieContainer;

                using (var response = (HttpWebResponse)request.GetResponse())
                {
                    xd.Load(response.GetResponseStream());
                    response.Close();
                }
            }
            catch (WebException e)
            {
                if (!this.hideConsole)
                {
                    Console.WriteLine(DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss") + ": Failed to retrieve page @ " + url);
                }
                this.errorMessages.Add(DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss") + ": Failed to retrieve page @ " + url);

                throw new ODWebException(e);
            }
            this.countRequest();
            return xd;
        }



        public List<string> GetCollectionRelations(string collection)
        {
            List<string> result = new List<string>();
            XmlDocument metadata = this.GetMetadata();


            return result;
        }


        protected static internal ODObject getObjectFromEntry(string collection, XmlElement entry, bool readOnly=false)
        {
            ODObject result = new ODObject(readOnly);
            result._data = ODBase.GetEntryFields(entry);
            result._binaryDataLink = ODBase.GetDataLink(entry);
            result._Collection = collection;
            result.Guid = result["Id"].ToString();
            return result;
        }


        public ODObject GetFirstItemByUniqueField(string collection, string field, string fieldValue, string mode="eq")
        {
            ODObject result = null;
            List<XmlElement> entries;
            switch (mode)
            {
                case "contains":
                    {
                        entries = this.GetPage(this._dataServiceUrl + collection + "Collection?$filter=substringof('" + fieldValue + "'," + field + ")");
                        break;
                    }
                case "eq":
                case "equals":
                default:
                    {
                        if (fieldValue != "true" && fieldValue != "false")
                        {
                            fieldValue = "'"+fieldValue+"'";
                        }
                        entries = this.GetPage(this._dataServiceUrl + collection + "Collection?$filter=" + field + " eq " + fieldValue);
                        break;
                    }
            }

            foreach (XmlElement entry in entries)
            {
                if (entry.Name == "entry")
                {
                    result = ODBase.getObjectFromEntry(collection, entry);                    
                    return result;
                }
            }
            return result;
        }

        public ODObject GetFirstItemByQuery(string collection, string query)
        {
            ODObject result = null;
            List<XmlElement> entries = this.GetPage(this._dataServiceUrl + collection + "Collection?$filter=" + query);
            foreach (XmlElement entry in entries)
            {
                if (entry.Name == "entry")
                {
                    result = ODBase.getObjectFromEntry(collection, entry);                    
                    return result;
                }
            }
            return result;
        }


        public List<ODObject> GetSomeItems(string collection, int skip)
        {
            return this.GetSomeItemsByQuery(collection, "", skip);
        }

        public List<ODObject> GetSomeItems(string collection)
        {
            return this.GetSomeItemsByQuery(collection, "", 0);
        }

        public List<ODObject> GetSomeItemsByQuery(string collection, string query)
        {
            return this.GetSomeItemsByQuery(collection, query, 0);
        }

        public List<ODObject> GetSomeItemsByQuery(string collection, string query, int skip)
        {
            List<ODObject> result = new List<ODObject>();
            List<XmlElement> entries = this.GetPage(this._dataServiceUrl + collection + "Collection?$filter=" + (query != "" ? query : "1 eq 1") + (skip > 0 ? "&$skip=" + skip : ""));
            foreach (XmlElement entry in entries)
            {
                if (entry.Name == "entry")
                {
                    ODObject item = ODBase.getObjectFromEntry(collection, entry);                    
                    result.Add(item);
                }
            }
            return result;
        }

        public List<ODObject> GetSomeLimitedItems(string collection, string fields, int skip)
        {
            return this.GetSomeLimitedItemsByQuery(collection, "", fields, skip);
        }

        public List<ODObject> GetSomeLimitedItems(string collection, string fields)
        {
            return this.GetSomeLimitedItemsByQuery(collection, "", fields, 0);
        }

        public List<ODObject> GetSomeLimitedItemsByQuery(string collection, string query, string fields)
        {
            return this.GetSomeLimitedItemsByQuery(collection, query, fields, 0);
        }

        public List<ODObject> GetSomeLimitedItemsByQuery(string collection, string query, string fields, int skip)
        {
            string toexpand="";
            if (!fields.StartsWith("Id,"))
            {
                fields = "Id," + fields;
            }
            if (fields.Contains("/"))
            {
                List<string> willexpand = new List<string>();
                foreach (string f in fields.Split(new char[] { ',',';', ' ' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    if (f.Contains("/"))
                    {
                        string obj = f.Substring(0, f.LastIndexOf("/"));
                        if (!willexpand.Contains(obj))
                        {
                            if (toexpand != "")
                            {
                                toexpand += ", ";
                            }
                            toexpand += obj;
                            willexpand.Add(obj);
                        }
                    }
                }
            }
            List<ODObject> result = new List<ODObject>();
            List<XmlElement> entries = this.GetPage(this._dataServiceUrl + collection + "Collection?$select="+fields+"&$filter=" + (query != "" ? query : "1 eq 1") + (skip > 0 ? "&$skip=" + skip : "")+(toexpand!=""?"&$expand="+toexpand:""));
            foreach (XmlElement entry in entries)
            {
                if (entry.Name == "entry")
                {
                    ODObject item = ODBase.getObjectFromEntry(collection, entry, true);
                    result.Add(item);
                }
            }
            return result;
        }



        public List<ODObject> GetAllItemsByQuery(string collection, string query, int maxIterations=10)
        {
            List<ODObject> result = new List<ODObject>();
            List<XmlElement> entries = this.GetAllPages(collection, query,"", maxIterations);
            
            foreach (XmlElement entry in entries)
            {
                if (entry.Name == "entry")
                {
                    ODObject item = ODBase.getObjectFromEntry(collection, entry);                    
                    result.Add(item);
                }
            }
            return result;
        }

        
        public List<ODObject> GetAllLimitedItemsByQuery(string collection, string query, string fields, int maxIterations = 10)
        {
  
            string toexpand = "";
            if (!fields.StartsWith("Id,"))
            {
                fields = "Id," + fields;
            }
            if (fields.Contains("/"))
            {
                List<string> willexpand = new List<string>();
                foreach (string f in fields.Split(new char[] { ',', ';', ' ' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    if (f.Contains("/"))
                    {
                        string obj = f.Substring(0, f.LastIndexOf("/"));
                        if (!willexpand.Contains(obj))
                        {
                            if (toexpand != "")
                            {
                                toexpand += ", ";
                            }
                            toexpand += obj;
                            willexpand.Add(obj);
                        }
                    }
                }
            }

            if (toexpand != "")
            {
                query += "&$expand=" + toexpand;
            }

            List<ODObject> result = new List<ODObject>();
            List<XmlElement> entries = this.GetAllPages(collection, query, fields, maxIterations);

            foreach (XmlElement entry in entries)
            {
                if (entry.Name == "entry")
                {
                    ODObject item = ODBase.getObjectFromEntry(collection, entry, true);
                    result.Add(item);
                }
            }
            return result;
        }

        public Dictionary<string, ODObject> GetDictionaryByUniqueField(string collection, string field, int maxIterations=10)
        {
            return this.GetDictionaryByUniqueField(collection, field, "", maxIterations);
        }

        public Dictionary<string, ODObject> GetDictionaryByUniqueField(string collection, string field, string query, int maxIterations=10)
        {
            List<XmlElement> entries = this.GetAllPages(collection, query, "", maxIterations);
            Dictionary<string, ODObject> result = new Dictionary<string, ODObject>();
            foreach (XmlElement entry in entries)
            {
                if (entry.Name == "entry")
                {
                    ODObject o = ODBase.getObjectFromEntry(collection, entry);                    

                    if (o._data.ContainsKey(field))
                    {
                        result[o[field].ToString()] = o;
                    }
                }
            }
            return result;
        }

        
    }

}
