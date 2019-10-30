function createDonutChart(data, node,legendHeight) {
  let donutChart = britecharts.donut(),
      donutContainer = d3.select(`.js-donut-chart-container-${node}`),
      legendChart = britecharts.legend(),
      colors = britecharts.colors.colorSchemas.britecharts,
      refWidth = 450,
      legendContainer;

  donutChart
    .isAnimated(true)
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
    .numberFormat(',.0f')
    .unit(' ' + data.unit)
    .colorSchema(colors);

  donutContainer.datum(data.data).call(donutChart);
  legendContainer = d3.select(`.js-legend-chart-container-${node}`);
  legendContainer.datum(data.data).call(legendChart);
}

function setChart(json, type, filter) {
  const container = document.getElementById('chart-container');
  container.innerHTML = '';
  let legendHeight = 150;

  Object.keys(json).forEach(function(key){
    let chartWrapper = document.createElement('div');
    let chartTitle = document.createElement('h4');
    let chartCard = document.createElement('div');
    let chart = document.createElement('div');
    let legend = document.createElement('div');

    chartTitle.innerText = `${type.selectedOptions[0].text} referred to ${filter.selectedOptions[0].text} for ${key}`;
    chartTitle.id = `title-${key}`;
    chartWrapper.classList.add('dashboard-chart-container', 'col-xl-6');
    chartCard.classList.add('card--chart');
    chart.classList.add(`js-donut-chart-container-${key}`, 'donut-chart-container');
    chart.id = `chart-${key}`;
    legend.classList.add(`js-legend-chart-container-${key}`, 'legend-chart-container');
    legend.id = `legend-${key}`;

    chartCard.appendChild(chartTitle);
    chartCard.appendChild(chart);
    chartCard.appendChild(legend);
    chartWrapper.appendChild(chartCard);
    container.appendChild(chartWrapper);

    createDonutChart(json[key], key, legendHeight);
  });
}
