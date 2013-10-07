using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Xml;
using System.Xml.Linq;
using System.Net.Cache;
using System.IO;

namespace BPMOData
{

    public class ODWebException : WebException
    {

        internal string _BPMmessage = string.Empty;
        internal string _BPMstacktrace = string.Empty;

        public string BPMmessage
        {
            get
            {
                return this._BPMmessage;
            }
        }

        public string BPMstacktrace
        {
            get
            {
                return this._BPMstacktrace;
            }
        }

        public ODWebException(WebException we)
            : base(we.Message, we.InnerException, we.Status, we.Response)
        {
            try
            {
                Stream s = we.Response.GetResponseStream();

                byte[] bytes = new byte[s.Length];
                int n = s.Read(bytes, 0, (int)s.Length);
                s.Close();

                string xml = Encoding.UTF8.GetString(bytes);

                XmlDocument xd = new XmlDocument();
                xd.LoadXml(xml);

                this._BPMmessage = xd["error"]["innererror"]["message"].InnerText;
                this._BPMstacktrace = xd["error"]["innererror"]["stacktrace"].InnerText;

            }
            catch { }

        }

    }
}
