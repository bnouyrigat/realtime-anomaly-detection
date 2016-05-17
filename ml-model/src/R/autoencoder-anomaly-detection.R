# Deep autoencoders used for anomaly detection
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Load the library
library(h2o)

# Connect to H2O
h2o_server = h2o.init(ip = "127.0.0.1", port = 54321, startH2O = FALSE)

train_ecg.hex = h2o.uploadFile(path="data/ecg_train.csv", header=F, sep=",", destination_frame="train_ecg.hex")
test_ecg.hex = h2o.uploadFile(path="data/ecg_test.csv", header=F, sep=",", destination_frame="test_ecg.hex")

# Train deep autoencoder learning model on "normal" training data, y ignored
anomaly_model = h2o.deeplearning(model_id = "DeepAutoEncoderModel", x=1:210,
                                 training_frame=train_ecg.hex, activation = "Tanh",
                                 classification_stop=-1, autoencoder=TRUE, hidden = c(50,20,50),
                                 l1=1E-4, epochs=100)

# Compute reconstruction error with the Anomaly detection app (MSE between output layer and input layer)
recon_error.hex = h2o.anomaly(anomaly_model, test_ecg.hex)

# Pull reconstruction error data into R and plot to find outliers (last 3 heartbeats)
recon_error = as.data.frame(recon_error.hex)
# Plot the reconstruction error and add a line for error in the 90th percentile
plot.ts(recon_error)
quantile  = h2o.quantile(recon_error.hex$Reconstruction.MSE)
threshold = quantile["90%"]
abline(h=threshold)
