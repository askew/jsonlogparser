# Json Log File Reader

This library is designed to help with processing large log files that contain many identical records 
in Json format. It supports either a valid JSON file containing an array of items, or the [JSON lines format][1] 
where overall the file is not valid JSON but contains a valid JSON value on each line of the file.

Currently there is just support for files stored in [Azure Blob storage][2] but other sources can easily be added 
be deriving a new class from 'JsonLogReader'.

The reader is designed to use data buffer efficiently so it can process very large files without a large memory overhead.

Simple transformation is possible by passing in an [`IDictionary<string,string>`][3] of key-value pairs. The keys should match
JSON property names in the source document. The records returned by the iterator will have these properties replaced by the 
values in the dictionary.


```CSharp
AzureBlobJsonLogReader br = new AzureBlobJsonLogReader(InputUri, FieldMapping, SourceJsonFormat.JsonLines)
{
    Progress = pcent => {
        Console.CursorLeft = 0;
        Console.Write($"Processing {pcent,3:d}%");
    }
};

foreach (string msg in br.GetRecords())
{
    ...
}

```


[1]: http://jsonlines.org/ "Documentation for the JSON Lines text file format"
[2]: https://azure.microsoft.com/services/storage/blobs/ "Azure Blob storage"
[3]: https://docs.microsoft.com/dotnet/api/system.collections.generic.idictionary-2 "IDictionary<TKey,TValue> Interface"