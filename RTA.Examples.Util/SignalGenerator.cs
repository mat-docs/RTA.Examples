// <copyright file="SignalGenerator.cs" company="McLaren Applied Ltd.">
// Copyright (c) McLaren Applied Ltd.</copyright>

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
        private const int MaxComponents = 4;
        private const double MinFrequency = 0.05;
        private const double MaxFrequency = 2;
        private const double MaxAmplitude = 800.0;
        private const double MaxValueOffset = 100.0;
        private const long MaxTimeOffset = 1000_000L;

        private readonly Component[] components;

        public SignalGenerator(Random rng)
        {
            components = new Component[rng.Next(MaxComponents - MinComponents + 1) + MinComponents];

            var maxAmplitude = MaxAmplitude;
            for (var i = 0; i < components.Length; i++)
            {
                var amplitude = rng.NextDouble() * maxAmplitude;
                var frequency = rng.NextDouble() * (MaxFrequency - MinFrequency) + MinFrequency;

                components[i] = new Component
                {
                    Amplitude = amplitude,
                    IntervalNanos = 1_000_000_000L / frequency,
                    TimeOffsetNanos = rng.NextDouble() * (MaxTimeOffset + 1000_000_000L),
                    ValueOffset = rng.NextDouble() * (MaxValueOffset * 2) - MaxValueOffset
                };

                maxAmplitude /= 2.0;
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
                    var value = Math.Sin(fraction * (2 * Math.PI)) * component.Amplitude + component.ValueOffset;
                    signal += value;
                }

                return signal;
            }
        }
    }
}