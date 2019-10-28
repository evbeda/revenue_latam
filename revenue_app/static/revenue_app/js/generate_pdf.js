function createCanvasData(){

  let canvasData = {
    'ars': {},
    'brl': {}
  }

  Object.keys(canvasData).forEach(function(key){

    canvasData[key]['title'] = document.getElementById(`title-${key}`);
    canvasData[key]['table'] = document.getElementById(`dynamic-table-${key}`);
    var svgChart = document.getElementById(`chart-top-${key}`).innerHTML;
    var svgLegend = document.getElementById(`legend-top-${key}`).innerHTML;

    svgChart = svgChart.replace(/\r?\n|\r/g, '').trim();
    svgLegend = svgLegend.replace(/\r?\n|\r/g, '').trim();

    var canvasChart = document.createElement('canvas');
    var canvasLegend = document.createElement('canvas');
    var contextChart = canvasChart.getContext('2d');
    var contextLegend = canvasLegend.getContext('2d');

    contextChart.clearRect(0, 0, canvasChart.width, canvasChart.height);
    contextLegend.clearRect(0, 0, canvasLegend.width, canvasLegend.height);
    canvg(canvasChart, svgChart);
    canvg(canvasLegend, svgLegend);

    canvasData[key]['imgChart'] = canvasChart.toDataURL('image/png');
    canvasData[key]['imgLegend'] = canvasLegend.toDataURL('image/png');

  });

  return canvasData;

};


function generateTopPDF(filename) {

  let navBar = document.getElementsByTagName("nav")[0];
  let canvasData = createCanvasData();

  var doc = new jsPDF('p', 'pt', 'a4');

  html2canvas(navBar)
  .then(canvas => {
    doc.internal.scaleFactor = 2.9;
    doc.addImage(canvas, 0, 0);
    doc.internal.scaleFactor = 3.6;
  })
  .then(canvas => html2canvas(canvasData.ars.title))
  .then(canvas => doc.addImage(canvas, 60, 45))
  .then(canvas => html2canvas(canvasData.ars.table))
  .then(canvas => {
    doc.addImage(canvas, 60, 85);
    doc.addImage(canvasData.ars.imgChart, 'PNG', 80, 245, 180, 180);
    doc.addImage(canvasData.ars.imgLegend, 'PNG', 280, 255, 180, 160);
  }).then(canvas => html2canvas(canvasData.brl.title))
  .then(canvas => doc.addImage(canvas, 60, 440))
  .then(canvas => html2canvas(canvasData.brl.table))
  .then(canvas => {
    doc.addImage(canvas, 60, 480);
    doc.addImage(canvasData.brl.imgChart, 'PNG', 80, 640, 180, 180);
    doc.addImage(canvasData.brl.imgLegend, 'PNG', 280, 650, 180, 160);
    doc.save(`${filename}_${getTimeNow()}.pdf`);
  });

};

function generateDashboardPDF() {
  let navBar = document.getElementsByTagName("nav")[0];
  var title = document.getElementById('title');
  var summarizedData = document.getElementById('summarized-data');
  var doc = new jsPDF('p', 'pt', 'a4');

  html2canvas(navBar)
  .then(canvas => {
    doc.internal.scaleFactor = 2.9;
    doc.addImage(canvas, 0, 0);
    doc.internal.scaleFactor = 3.6;
  })
  .then(canvas => html2canvas(title))
  .then(canvas => doc.addImage(canvas, 60, 70))
  .then(canvas => html2canvas(summarizedData))
  .then(canvas => doc.addImage(canvas, 60, 110))
  .then(canvas => doc.save(`dashboard_${getTimeNow()}.pdf`));

};

function generateDetailPDF(filename) {
  let navBar = document.getElementsByTagName("nav")[0];
  var title = document.getElementById('title');
  var summarizedData = document.getElementById('summarized-data');
  var doc = new jsPDF('p', 'pt', 'a4');

  html2canvas(navBar)
  .then(canvas => {
    doc.internal.scaleFactor = 2.9;
    doc.addImage(canvas, 0, 0);
    doc.internal.scaleFactor = 3.2;
  })
  .then(canvas => html2canvas(title))
  .then(canvas => doc.addImage(canvas, 40, 70))
  .then(canvas => html2canvas(summarizedData))
  .then(canvas => doc.addImage(canvas, 40, 110))
  .then(canvas => doc.save(`${filename}_${getTimeNow()}.pdf`));

};

function generateChartPDF() {
  let navBar = document.getElementsByTagName("nav")[0];

  var chartContainer = document.getElementById('chart-container');
  var doc = new jsPDF('p', 'pt', 'a4');

  html2canvas(navBar)
  .then(canvas => {
    doc.internal.scaleFactor = 2.9;
    doc.addImage(canvas, 0, 0);
    doc.internal.scaleFactor = 3.2;
  })
  .then(canvas => html2canvas(chartContainer))
  .then(canvas => {
    doc.addImage(canvas, 40, 70);
    doc.save(`dashboard_charts_${getTimeNow()}.pdf`);
  });

}

function getTimeNow() {
  return new Date().toLocaleString('fr-CA').replace(' h ', 'h').replace(" min ", "m").replace(" s", "s").replace(" ", "_");
}
