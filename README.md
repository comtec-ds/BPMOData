# BPMOData
Proxy class methods for accessing EntityDateService of BPMonline



# Some samples:

ODBase mybase = new ODBase("http://yoursite","yourlogin", "yourpassword");

ODObject iKnowById = new ODObject(mybase, "Contact", "F6D073E8-6CAC-4CD5-89B8-D2220D760241");

ODObject iWantToFind = mybase.GetFirstItemByQuery("Contact", "Name eq 'Ivanov'");
iWantToFind["Name"] = "Petrov";
iWantToFind.Update(bpm);

ODObject newObj = ODObject.NewObject("Contact");
newObj["Name"] = "Ivanov";
newObj.Update(bpm);

List<ODObject> some = bpm.GetSomeItemsByQuery("Contact", "Sex/Name eq 'Male'");
