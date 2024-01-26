namespace TrainSchedule.Ivu.Worker.Model.Repositories;

public interface IStorageRepository
{
    IList<BlobItem> ListLastestPlanningFiles();
    IvuBlobData DownloadFile(BlobItem file);
    IvuBlobData DownloadFile(BlobItem file, string localPath);
}

public class StorageRepository : IStorageRepository
{
    private readonly Lazy<Uri> _url;

    public StorageRepository(IConfiguration configuration)
    {
        _url = new Lazy<Uri>(() => new Uri(configuration.GetValue<string>("ivu-data-storage-container-read")));
    }

    public IList<BlobItem> ListLastestPlanningFiles()
    {
        var client = new BlobContainerClient(_url.Value, new DefaultAzureCredential());
        var blobs = client.GetBlobs(prefix: "train-formation-planning");

        return blobs.Where(x => (DateTime.Today - x.Properties.LastModified)?.Days < 1).ToList();
    }

    // TODo: enable before push
     public IvuBlobData DownloadFile(BlobItem file)
     {
         var client = new BlobContainerClient(_url.Value, new DefaultAzureCredential());
    
         var blobClient = client.GetBlobClient(file.Name);
         
         return new IvuBlobData(file.Name, blobClient.DownloadContent().Value.Content.ToArray());
     }

    //Remove this method when the above is enabled
    public IvuBlobData DownloadFile(BlobItem file, string localPath)
    {
        // Ensure that localPath ends with a backslash so it's treated as a folder path
        if (!localPath.EndsWith(Path.DirectorySeparatorChar.ToString(), StringComparison.Ordinal))
            localPath += Path.DirectorySeparatorChar;

        // Construct full local path where blob will be saved.
        var filePath = Path.Combine(localPath, file.Name);

        // Check if directory exists; if not, create it.
        var directory = Path.GetDirectoryName(filePath);
    
        if (!Directory.Exists(directory))
            Directory.CreateDirectory(directory);  // This creates all directories and subdirectories
        
        // Check if the file already exists at that path.
        if (File.Exists(filePath))
        {
            // The file already exists on disk; no need to download it again.
            return new IvuBlobData(file.Name, File.ReadAllBytes(filePath));
        }
        
        // If we reach here, it means we need to download the blob as it doesn't exist locally yet.
        
        var client = new BlobContainerClient(_url.Value, new DefaultAzureCredential());
        
        var blobClient = client.GetBlobClient(file.Name);
        
        // Downloading content from Azure Blob Storage
        var response = blobClient.DownloadContent();
         
        byte[] contentArray= response.Value.Content.ToArray();
         
        try 
        {
            File.WriteAllBytes(filePath,contentArray); 
             
            return new IvuBlobData(file.Name,contentArray);
             
        }catch(Exception ex)
        {
            Console.WriteLine($"An error occurred while writing {file.Name}to disk: {ex.Message}");
            throw;
        }
           
    }

    
   
}