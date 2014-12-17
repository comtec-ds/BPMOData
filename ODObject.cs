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

    public class ODObject
    {
        //{00000000-0000-0000-0000-000000000000}
        public string Guid = string.Empty;
        public bool hasBinaryData
        {
            get
            {
                return this._binaryDataLink != "";
            }
        }

        internal string _Collection = string.Empty;
        internal Boolean IsReadOnly = false;
        public string _binaryDataLink = string.Empty;

        internal Dictionary<string, object> _data = new Dictionary<string, object>();


        public bool exists
        {
            get
            {
                return this._data != null && this._data.Count > 0 && this.Guid != string.Empty;
            }
        }

        public object this[string what]
        {
            get
            {
                if (this._data.ContainsKey(what))
                {
                    return this._data[what];
                }
                return null;
            }
            set
            {
                this._data[what] = value;
            }
        }

        public override string ToString()
        {
            return this.Guid;
        }

        internal void FixDateBug()
        {
            List<string> keys = new List<string>(this._data.Keys);
            foreach (string k in keys)
            {
                if (this._data[k] != null)
                {
                    string ts = this._data[k].ToString();
                    if (ts.EndsWith("T00:00:00.0000000Z") || ts.EndsWith("T00:00:00"))
                    {
                        this._data[k] = ts.Replace("T00:00:00", "T12:00:00");
                    }
                }
            }
        }

        public bool HasProperty(string prop)
        {
            return this._data.ContainsKey(prop);
        }

        public List<string> Properties
        {
            get
            {
                List<string> result = new List<string>();
                foreach (string k in this._data.Keys)
                {
                    result.Add(k);
                }
                return result;
            }
        }

        public Dictionary<string, string> Data
        {
            get
            {
                Dictionary<string, string> result = new Dictionary<string, string>();
                foreach (KeyValuePair<string, object> kvp in this._data)
                {
                    switch (kvp.Value.GetType().Name)
                    {
                        case "DateTime":
                            {
                                result[kvp.Key] = ((DateTime)kvp.Value).ToString("o");
                                break;
                            }
                        default:
                            {
                                result[kvp.Key] = kvp.Value.ToString();
                                break;
                            }
                    }
                }
                return result;
            }
        }
        public string Collection
        {
            get
            {
                return this._Collection;
            }
        }
        public string CollectionLocalName
        {
            get
            {
                if (this.Collection.EndsWith("Collection") || this.Collection.EndsWith("CollectionVersion"))
                {
                    return this._Collection;
                }
                else
                {
                    return this._Collection + "Collection";
                }
            }
        }
        
        public Dictionary<string, List<ODObject>> Many = new Dictionary<string, List<ODObject>>();

        /// <summary>
        /// loads Dictionary "Many" with links to this object
        /// </summary>
        public void LoadMany(ODBase odbase, string Collection)
        {
            this.LoadMany(odbase, Collection, this._Collection + "/Id");
        }

        /// <summary>
        /// loads Dictionary "Many" with links to this object
        /// </summary>
        public void LoadMany(ODBase odbase, string Collection, string joinField)
        {
            List<XmlElement> entries = odbase.GetAllPages(Collection, joinField + " eq guid'" + this.Guid + "'", "");
            List<ODObject> result = new List<ODObject>();
            foreach (XmlElement entry in entries)
            {
                if (entry.Name == "entry")
                {
                    ODObject o = new ODObject();
                    o._data = ODBase.GetEntryFields(entry);
                    o._Collection = Collection;
                    o.Guid = o["Id"].ToString();
                    result.Add(o);
                }
            }

            if (result.Count > 0)
            {
                this.Many[Collection] = result;
            }
            else
            {
                this.Many.Remove(Collection);
            }
        }

        /// <summary>
        /// Cleans temporary data with keys starting with exclamation mark. 
        /// </summary>
        public Dictionary<string, object> CleanTemp()
        {
            Dictionary<string, object> result = new Dictionary<string, object>();
            List<string> toRemove = new List<string>();
            foreach (KeyValuePair<string, object> kvp in this._data)
            {
                if (kvp.Key.StartsWith("!"))
                {
                    toRemove.Add(kvp.Key);
                }
            }
            foreach (string k in toRemove)
            {
                result[k] = this._data[k];
                this._data.Remove(k);
            }
            return result;
        }

        /// <summary>
        /// Finds and initializes object. Throws exception if not found.
        /// </summary>
        public ODObject(ODBase odb, string Collection, string Guid)
        {
            this.Guid = Guid;
            this._Collection = Collection;

            this._data = null;

            List<XmlElement> elements = odb.GetPage(odb.GetCollectionPageUrl(Collection) + "Collection(guid'" + Guid + "')");
            if (elements.Count == 1)
            {
                this._data = ODBase.GetEntryFields(elements[0]);
            }

            if (this._data == null || this._data.Count == 0)
            {
                throw new Exception(Collection + "Collection(guid'" + Guid + "') not found");
            }
            
            this._binaryDataLink = ODBase.GetDataLink(elements[0]);
            this.IsReadOnly = false;
        }

        public byte[] GetData(ODBase odb)
        {
            if (this.hasBinaryData)
            {
                return odb.GetData(odb._dataServiceUrl+this._binaryDataLink);
            }
            else
            {
                return new byte[0];
            }
        }

        public byte[] GetData(ODBase odb, bool useVwFile)
        {
            byte[] standartData = new byte[0];

            try
            {
                standartData = this.GetData(odb);
            }
            catch (ODWebException exc)
            {
                if (!useVwFile)
                {
                    throw exc;
                }
            }

            if (!useVwFile)
            {
                return standartData;
            }
            else
            {
                ODObject vw = new ODObject(odb, "VwFile", this.Guid);
                if (vw != null && vw.hasBinaryData)
                {
                    return vw.GetData(odb);
                }
            }
            return standartData;
        }

        protected internal ODObject(bool readOnly=false)
        {
            this.IsReadOnly = readOnly;
        }

        /// <summary>
        /// Prepares new object in Collection, CreatedOn and Modified will be set to DateTime.Now
        /// Call Update method for save
        /// </summary>
        public static ODObject NewObject(string Collection)
        {
            ODObject result = new ODObject();
            result._Collection = Collection;
            result._data = new Dictionary<string, object>();
            result._data["CreatedOn"] = DateTime.Now.ToUniversalTime().ToString("o");
            result._data["ModifiedOn"] = DateTime.Now.ToUniversalTime().ToString("o");
            return result;
        }

        /// <summary>
        /// Creates or updates object
        /// </summary>
        public string Update(ODBase odb)
        {
            if (!this.IsReadOnly)
            {
                this.FixDateBug();
                this["ModifiedOn"] = DateTime.Now.ToUniversalTime().ToString("o");
                if (this.Guid == string.Empty)
                {
                    // to create
                    return odb.AddItem(this._Collection, this._data);
                }
                else
                {
                    // to update
                    return odb.UpdateItem(this._Collection, this.Guid, this._data);
                }
            }
            else
            {
                throw new ODSecurityException("update");
            }
        }

        
        /// <summary>
        /// Deletes object. Not disposing. Calling Update method will recreate object
        /// </summary>
        public void Delete(ODBase odb)
        {
            if (!this.IsReadOnly)
            {
                if (this.Guid != string.Empty)
                {
                    odb.DeleteItem(this._Collection, this.Guid);
                    this.Guid = string.Empty;
                    this["Id"] = null;
                }
            }
            else
            {
                throw new ODSecurityException("delete");
            }
        }

        public bool UploadBinary(ODBase odb, byte[] bytes, bool saveFiletypeAndSize = true, bool saveSHA256Hash = true)
        {
            if (!this.IsReadOnly)
            {
                string result1 = odb.UploadBinary(this._Collection, this.Guid, bytes, saveSHA256Hash);
                if (result1 != "")
                {
                    if (saveFiletypeAndSize || saveSHA256Hash)
                    {
                        if (saveFiletypeAndSize)
                        {
                            this["Size"] = bytes.Length;
                            this["TypeId"] = ODBase.CommonIds.fileTypeFile;
                        }
                        if (saveSHA256Hash)
                        {
                            this["Hash"] = result1;
                        }
                        this.Update(odb);
                    }
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                throw new ODSecurityException("uploadbinary");
            }
        }

        public void DeleteLink(ODBase odb, string CollectionTo)
        {
            if (!this.IsReadOnly)
            {
                odb.DeleteLink(this._Collection, this.Guid, CollectionTo);
                if (this.HasProperty(CollectionTo + "Id"))
                {
                    this[CollectionTo + "Id"] = null;
                }
            }
            else
            {
                throw new ODSecurityException("deletelink");
            }
        }

        /// <summary>
        /// deprecated
        /// </summary>
        public void MergeWith(ODObject second)
        {
            if (!this.IsReadOnly)
            {
                Dictionary<string, object> secondData = new Dictionary<string, object>();
                foreach (KeyValuePair<string, object> kvp in second._data)
                {
                    if (kvp.Key != "Id")
                    {
                        secondData.Add(kvp.Key, kvp.Value);
                    }
                }
                this.MergeWith(secondData);
            }
            else
            {
                throw new ODSecurityException("update");
            }
        }

        /// <summary>
        /// deprecated
        /// </summary>
        public void MergeWith(Dictionary<string, object> newData)
        {
            if (!this.IsReadOnly)
            {
                foreach (KeyValuePair<string, object> kvp in newData)
                {
                    if (this._data.ContainsKey(kvp.Key))
                    {
                        if (kvp.Value == null)
                        {
                            this._data.Remove(kvp.Key);
                        }
                        else
                        {
                            this._data[kvp.Key] = kvp.Value;
                        }
                    }
                    else
                    {
                        if (kvp.Value != null)
                        {
                            this._data[kvp.Key] = kvp.Value;
                        }
                    }
                }
            }
            else
            {
                throw new ODSecurityException("update");
            }
        }
                

    }


    

}
