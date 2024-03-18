package promremotewrite

import (
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmagent/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmagent/remotewrite"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
	parserCommon "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/promremotewrite/stream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/tenantmetrics"
	"github.com/VictoriaMetrics/metrics"
)

var (
	rowsInserted        = metrics.NewCounter(`vmagent_rows_inserted_total{type="promremotewrite"}`)
	rowsTenantInserted  = tenantmetrics.NewCounterMap(`vmagent_tenant_inserted_rows_total{type="promremotewrite"}`)
	rowsPerInsert       = metrics.NewHistogram(`vmagent_rows_per_insert{type="promremotewrite"}`)
	histogramsInserted  = metrics.NewCounter(`vmagent_histograms_inserted_total{type="promremotewrite"}`)
	histogramsPerInsert = metrics.NewHistogram(`vmagent_historams_per_insert{type="promremotewrite"}`)
)

// InsertHandler processes remote write for prometheus.
func InsertHandler(at *auth.Token, req *http.Request) error {
	extraLabels, err := parserCommon.GetExtraLabels(req)
	if err != nil {
		return err
	}
	isVMRemoteWrite := req.Header.Get("Content-Encoding") == "zstd"
	return stream.Parse(req.Body, isVMRemoteWrite, func(tss []prompb.TimeSeries) error {
		return insertRows(at, tss, extraLabels)
	})
}

func insertRows(at *auth.Token, timeseries []prompb.TimeSeries, extraLabels []prompbmarshal.Label) error {
	ctx := common.GetPushCtx()
	defer common.PutPushCtx(ctx)

	rowsTotal := 0
	histogramsTotal := 0
	tssDst := ctx.WriteRequest.Timeseries[:0]
	labels := ctx.Labels[:0]
	samples := ctx.Samples[:0]
	histograms := ctx.Histograms[:0]
	for i := range timeseries {
		ts := &timeseries[i]
		histogramsTotal += len(ts.Histograms)
		// Lump both together for now
		rowsTotal += len(ts.Samples) + histogramsTotal
		labelsLen := len(labels)
		for i := range ts.Labels {
			label := &ts.Labels[i]
			labels = append(labels, prompbmarshal.Label{
				Name:  label.Name,
				Value: label.Value,
			})
		}
		labels = append(labels, extraLabels...)
		samplesLen := len(samples)
		for i := range ts.Samples {
			sample := &ts.Samples[i]
			samples = append(samples, prompbmarshal.Sample{
				Value:     sample.Value,
				Timestamp: sample.Timestamp,
			})
		}
		histogramsLen := len(histograms)
		for i := range ts.Histograms {
			histogram := &ts.Histograms[i]
			histograms = append(histograms, prompbmarshal.Histogram{
				Count:          histogram.Count,
				Sum:            histogram.Sum,
				Schema:         histogram.Schema,
				ZeroThreshold:  histogram.ZeroThreshold,
				ZeroCount:      histogram.ZeroCount,
				NegativeSpans:  prompbmarshal.FromPrompb(histogram.NegativeSpans),
				NegativeDeltas: histogram.NegativeDeltas,
				PositiveSpans:  prompbmarshal.FromPrompb(histogram.PositiveSpans),
				PositiveDeltas: histogram.PositiveDeltas,
				ResetHint:      prompbmarshal.ResetHint(histogram.ResetHint),
				Timestamp:      histogram.Timestamp,
			})
		}
		tssDst = append(tssDst, prompbmarshal.TimeSeries{
			Labels:     labels[labelsLen:],
			Samples:    samples[samplesLen:],
			Histograms: histograms[histogramsLen:],
		})
	}
	ctx.WriteRequest.Timeseries = tssDst
	ctx.Labels = labels
	ctx.Samples = samples
	ctx.Histograms = histograms
	if !remotewrite.TryPush(at, &ctx.WriteRequest) {
		return remotewrite.ErrQueueFullHTTPRetry
	}
	rowsInserted.Add(rowsTotal)
	histogramsInserted.Add(histogramsTotal)
	if at != nil {
		rowsTenantInserted.Get(at).Add(rowsTotal)
	}
	rowsPerInsert.Update(float64(rowsTotal))
	histogramsPerInsert.Update(float64(histogramsTotal))
	return nil
}
