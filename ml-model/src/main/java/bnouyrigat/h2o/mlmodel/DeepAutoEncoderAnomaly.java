package bnouyrigat.h2o.mlmodel;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class DeepAutoEncoderAnomaly {

    private final DeepAutoEncoderModel deepAutoEncoderModel;

    public DeepAutoEncoderAnomaly(DeepAutoEncoderModel deepAutoEncoderModel) {
        this.deepAutoEncoderModel = deepAutoEncoderModel;
    }

    public double scoreAutoEncoder(double[] data) {
        double[] preds = new double[data.length];
        deepAutoEncoderModel.score0(data, preds);

        return mse(data, preds);
    }

    public static double mse(double[] expected, double[] prediction) {
        if (expected.length == 0 || expected.length != prediction.length) {
            throw new IllegalArgumentException();
        }
        double result = 0.0;
        for (int i = 0; i < expected.length; i++) {
            result += Math.pow(expected[i] - prediction[i], 2);
        }
        result /= expected.length;
        return result;
    }

    public static BigDecimal mseBigDecimal(double[] expected, double[] prediction) {
        if (expected.length == 0 || expected.length != prediction.length) {
            throw new IllegalArgumentException();
        }
        BigDecimal result = new BigDecimal(0.0);
        for (int i = 0; i < expected.length; i++) {
            BigDecimal diff = BigDecimal.valueOf(expected[i])
                    .subtract(BigDecimal.valueOf(prediction[i]));
            result = result.add(diff.pow(2));
        }
        BigDecimal l = BigDecimal.valueOf(expected.length);
        return result.divide(l, RoundingMode.HALF_UP);
    }
}
