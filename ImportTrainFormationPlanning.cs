using ODS.Common.Extensions;
using TrainSchedule.DataModel.Models;
using TrainSchedule.Ivu.Worker.Model;
using TrainSchedule.Ivu.Worker.Model.Repositories;
using TrainSchedule.Ivu.Worker.Model.TrainSchedule;

namespace TrainSchedule.Ivu.Worker.Jobs.TrainFormationPlanning;

/*
 Why do we need this worker?

 To be able to handle more than 24 hours of scheduled trains, we need to get information about the planned trains.
 At the moment, information about scheduled trains are coming from IVU (Dispatch), but we need to get information about the
 planned trains from the IVU Export, so we can supply a more long term time schedule.

 The IVU Export is a file that is uploaded to Azure Blob Storage every night. The files are very large files, therefore
 we have decided to handle them in this worker and not in the IVU data project. Also it would generate a lot of traffic
 in form of a lot of messages.
 */

public interface IImportTrainFormationPlanning
{
    void Execute(CancellationToken token);
}

public class ImportTrainFormationPlanning(
    ILogger<ImportTrainFormationPlanning> logger,
    IStorageRepository storageRepository,
    IIncomingMessageParser messageParser,
    IMetricsService metricsService,
    ITrainFormationMapper trainFormationMapper,
    IGetPendingFiles getPendingFiles,
    IProcessingLogRepository processingLogRepository,
    IScheduledTrainRepository scheduledTrainRepository,
    IIvuTrainScheduleMapper ivuTrainScheduleMapper,
    IPersistTrain persistTrain,
    IGetPlannedTrainsToCancel getPlannedTrainsToCancel,
    IMessageProducer messageProducer)
    : IImportTrainFormationPlanning
{
    private readonly Counter _trainsToSave = Metrics.CreateCounter(
        "trainschedule_ivu_worker_importTrainFormationPlanning_trains_to_save",
        "Measuring the number of trains to save in one transaction");
    public void Execute(CancellationToken token)
    {
        if (token.IsCancellationRequested)
        {
            return;
        }

        logger.LogInformation("Starting scheduled execution of {Name}", nameof(ImportTrainFormationPlanning));

        try
        {
            metricsService.ExecuteJob(() => ProcessBlobPlanningQueue(token), "import-trainformation-planning",
                "execute");
        }
        catch (Exception e)
        {
            logger.LogError(e, "Execution of {Name} failed - will retry later", nameof(ImportTrainFormationPlanning));
        }
    }

    private void ProcessBlobPlanningQueue(CancellationToken token)
    {
        var pendingFiles = getPendingFiles.Execute();

        if (!pendingFiles.Any())
        {
            logger.LogInformation("No pending files to process");
            return;
        }

        pendingFiles = pendingFiles.OrderBy(x => x.Properties.LastModified).ToList();

        logger.LogInformation("Processing messages in {Count} new files", pendingFiles.Count);
        pendingFiles.ToList().ForEach(x => ProcessNewBlobs(x, token));

        logger.LogInformation("Finished processing messages in {Count} new files", pendingFiles.Count);
    }


    private void ProcessNewBlobs(BlobItem blob, CancellationToken token)
    {
        if (token.IsCancellationRequested)
        {
            return;
        }

        logger.LogDebug("Processing messages in {Name}", blob.Name);
//TODO: Remove before push
        var blobData = storageRepository.DownloadFile(blob, "c:\\temp\\ivudata");
        //var blobData = storageRepository.DownloadFile(blob);
        
        
        var xml = messageParser.Execute(blobData.FileContents);

        try
        {
            metricsService.ExecuteJob(() => ProcessTrainFormations(xml, blobData.BlobName), "process_train_formations",
                "train_formation");

            var processingLog = new ProcessingLog
            {
                Path = blob.Name,
                JobType = JobType.ParseIvuTrainFormationPlanning,
                Status = ProcessStatus.Done,
                Modified = SystemTime.UtcNow()
            };
            processingLogRepository.Create(processingLog);
        }
        catch (Exception e)
        {
            logger.LogError(e, "Failed processing Queue Item");
            var processingLog = new ProcessingLog
            {
                Path = blob.Name,
                JobType = JobType.ParseIvuTrainFormationPlanning,
                Status = ProcessStatus.Failed,
                Modified = SystemTime.UtcNow()
            };
            processingLogRepository.Create(processingLog);
        }
    }

    private void ProcessTrainFormations(IEnumerable<XmlNode> xmlNodes, string blobName)
    {
        var trainFormations = trainFormationMapper.Map(xmlNodes, blobName).ToList();
        if (!trainFormations.Any())
        {
            return;
        }

        try
        {
            logger.LogInformation($"Successfully found {trainFormations.Count} TrainFormations");
            var allStartDates = trainFormations.Select(x => x.Trains.Select(y => y.StartDate).Distinct()).ToList();
            var scheduledTrainsForAllDates =
                scheduledTrainRepository.GetScheduledTrainsForAllDates(allStartDates).ToList();
            var canceledTrainsFromPlanning =
                getPlannedTrainsToCancel.Execute(scheduledTrainsForAllDates, trainFormations).ToList();

            //Create an empty list of trains
            //Instead of saving in ProcessTrainFormation, we save all trains in one transaction
            var trains = new List<RelationalTrain>();

            foreach (var trainFormation in trainFormations)
            {
                try
                {
                    trains.Add(ProcessTrainFormation(trainFormation, scheduledTrainsForAllDates));
                    _trainsToSave.Inc();
                }
                catch (Exception e)
                {
                    logger.LogError(e, "Failed processing TrainFormation {trainNumber}", trainFormation.TrainNumber);
                }
            }

          //  using var scope = TransactionUtils.CreateTransactionScope();
            
            var events = new List<object>();
            
            persistTrain.BulkInsertOrUpdate(trains, scheduledTrainsForAllDates, events);
            
            //scope.Complete();
            logger.LogInformation("********** Saving {Count} trains complete **************", trains.Count);
            
            if (events.Any())
            {
                messageProducer.SendMessages(events);
                logger.LogInformation("********** Sending {Count} events complete **************", events.Count);
            }

            try
            {
                ProcessCanceledTrains(canceledTrainsFromPlanning);
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception);
                throw;
            }
        }
        catch (Exception e)
        {
            e.WithContext("FilePath", blobName);
            logger.LogError(e, "Failed producing event for TrainFormation");
        }
    }

    private void ProcessCanceledTrains(List<ScheduledTrain> canceledTrainsFromPlanning)
    {
        var events = new List<object>();

        using var scope = TransactionUtils.CreateTransactionScope();
        foreach (var scheduledTrain in canceledTrainsFromPlanning)
        {
            try
            {
                var canceledTrain = new RelationalTrain
                {
                    ScheduledTrain = scheduledTrain
                };
                persistTrain.Cancel(canceledTrain, events);
                logger.LogInformation("Cancel train {scheduledTrain}", scheduledTrain);
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed cancelling train {scheduledTrain}", scheduledTrain);
            }
        }

        scope.Complete();

        if (events.Any())
        {
            messageProducer.SendMessages(events);
        }
    }

    private RelationalTrain ProcessTrainFormation(TrainFormation trainFormation,
        IEnumerable<ScheduledTrain> scheduledTrainsFromDb)
    {
        var invalid = trainFormation.Trains.Where(x => x.TrainSections != null)
            .Any(x => x.TrainSections.Any(y => y.Division.ToUpper() != "STOG"));

        if (invalid)
        {
            logger.LogInformation(
                "Train formation planning for train {TrainNumber} {ScheduleDate:yyyy-MM-dd} in blob {SourceBlob} has no train sections that contains STOG, skipping.",
                trainFormation.TrainNumber, trainFormation.StartDate, trainFormation.SourceBlob);
            return null;
        }

        var relevant = trainFormation.Trains.FirstOrDefault(x => x.TripType == "PassengerTrip" || x.IsCanceled);
        if (relevant == null)
        {
            logger.LogInformation(
                "Train formation planning for train {TrainNumber} {ScheduleDate:yyyy-MM-dd} in blob {SourceBlob} has no passenger trips, skipping.",
                trainFormation.TrainNumber, trainFormation.StartDate, trainFormation.SourceBlob);
            return null;
        }

        if (TrainAllReadyExistAndIsNotManagedByPlanning(relevant, scheduledTrainsFromDb))
        {
            logger.LogInformation(
                "Train {ScheduledTrain} already exists in the database and is not managed by planning",
                relevant.ToString());
            return null;
        }

        var train = ivuTrainScheduleMapper.MapTrain(relevant);
        train.ScheduledTrain.Source = trainFormation.SourceBlob;

        return train;
    }

    private bool TrainAllReadyExistAndIsNotManagedByPlanning(Train train,
        IEnumerable<ScheduledTrain> scheduledTrainsFromDb)
    {
        if (!scheduledTrainsFromDb.Any(x =>
                x.TrainNumber == train.TrainNumber &&
                x.ScheduledDate == train.StartDate.ToDateOnly() &&
                x.OperatorId ==
                101 && //This is done because we don't have the number representation of the train before mapping.
                x.SourceType != SourceType.Planning &&
                !x.IsDeleted))
        {
            return false;
        }

        //Metric? 
        return true;
    }
}