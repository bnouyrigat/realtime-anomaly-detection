package bnouyrigat.h2o.processor;

import bnouyrigat.h2o.mlmodel.DeepAutoEncoderAnomaly;
import bnouyrigat.h2o.mlmodel.DeepAutoEncoderModel;
import org.apache.kafka.streams.processor.AbstractProcessor;

public class AnomalyDetectionProcessor extends AbstractProcessor<String, ECGData> {

    private DeepAutoEncoderAnomaly deepAutoEncoderAnomaly = new DeepAutoEncoderAnomaly(new DeepAutoEncoderModel());

    private double anomalyThreshold = 0.1d;

    @Override
    public void process(String key, ECGData value) {
        double mse = deepAutoEncoderAnomaly.scoreAutoEncoder(value.getDataArray());
        if (mse > anomalyThreshold) {
            context().forward(key, value);
        }
        context().commit();
    }

}
