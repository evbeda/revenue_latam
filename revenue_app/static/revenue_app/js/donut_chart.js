function createDonutChart(data, currency,legendHeight) {
  let donutChart = britecharts.donut(),
      donutContainer = d3.select(`.js-donut-chart-container-${currency}`),
      legendChart = britecharts.legend(),
      colors = britecharts.colors.colorSchemas.britecharts,
      refWidth = 450,
      legendContainer;

  donutChart
    .isAnimated(true)
    .highlightSliceById(2).hasFixedHighlightedSlice(true)
    .width(refWidth)
    .height(refWidth)
    .externalRadius(refWidth/2.5)
    .internalRadius(refWidth/5)
    .colorSchema(colors)
    .on('customMouseOver', function(data) {
    legendChart.highlight(data.data.id);
  })
    .on('customMouseOut', function() {
    legendChart.clearHighlight();
  });

  legendChart
    .width(refWidth)
    .height(legendHeight)
    .numberFormat('s')
    .colorSchema(colors);

  donutContainer.datum(data.data).call(donutChart);
  legendContainer = d3.select(`.js-legend-chart-container-${currency}`);
  legendContainer.datum(data.data).call(legendChart);

  donutChart.highlightSliceById(3).isAnimated(true);
  donutContainer.datum(data.data).call(donutChart);

}
