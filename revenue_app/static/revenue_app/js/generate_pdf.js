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


function generatePDF() {

  let canvasData = createCanvasData()

  var doc = new jsPDF('p', 'pt', 'a4');
  doc.internal.scaleFactor = 3.6;

  html2canvas(canvasData.ars.title)
  .then(canvas => doc.addImage(canvas, 60, 40))
  .then(canvas => html2canvas(canvasData.ars.table))
  .then(canvas => {
    doc.addImage(canvas, 60, 80);
    doc.addImage(canvasData.ars.imgChart, 'PNG', 80, 240, 180, 180);
    doc.addImage(canvasData.ars.imgLegend, 'PNG', 280, 250, 180, 160);
  }).then(canvas => html2canvas(canvasData.brl.title))
  .then(canvas => doc.addImage(canvas,60, 440))
  .then(canvas => html2canvas(canvasData.brl.table))
  .then(canvas => {
    doc.addImage(canvas, 60, 480);
    doc.addImage(canvasData.brl.imgChart, 'PNG', 80, 640, 180, 180);
    doc.addImage(canvasData.brl.imgLegend, 'PNG', 280, 650, 180, 160);
    doc.save('top_organizers.pdf');
  });

};
