using System;

namespace RTA.Examples.Util
{
    public class SignalGenerator
    {
        private struct Component
        {
            public double IntervalNanos;
            public double TimeOffsetNanos;
            public double Amplitude;
            public double ValueOffset;
        }

        private const int MinComponents = 1;
        private const int MaxComponents = 3;
        private const double MinFrequency = 0.01;
        private const double MaxFrequency = 5000.0;
        private const double MaxAmplitude = 300.0;
        private const double MaxValueOffset = 50.0;
        private const long MaxTimeOffset = 10;

        private readonly Component[] components;

        public SignalGenerator(Random rng)
        {
            components = new Component[rng.Next(MaxComponents - MinComponents + 1) + MinComponents];

            var maxAmplitude = MaxAmplitude;
            var maxFrequency = MaxFrequency;
            for (var i = 0; i < components.Length; i++)
            {
                var amplitude = maxAmplitude = rng.NextDouble() * maxAmplitude;
                var frequency = maxFrequency = rng.NextDouble() * (maxFrequency - MinFrequency) + MinFrequency;

                components[i] = new Component
                {
                    Amplitude = amplitude,
                    IntervalNanos = 1_000_000_000L / frequency,
                    TimeOffsetNanos = rng.NextDouble() * (MaxTimeOffset * 1000_000_000L),
                    ValueOffset = rng.NextDouble() * (MaxValueOffset * 2) - MaxValueOffset
                };
            }
        }

        public double this[long timeNanos]
        {
            get
            {
                var signal = 0.0;
                for (var i = 0; i < components.Length; i++)
                {
                    var component = components[i];
                    var fraction = (timeNanos + component.TimeOffsetNanos) / component.IntervalNanos;
                    var value = Math.Sin(2 * Math.PI * fraction) * component.Amplitude + component.ValueOffset;
                    signal += value;
                }

                return signal;
            }
        }
    }
}