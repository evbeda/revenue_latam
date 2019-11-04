function createCanvasData(ars, brl){

  let canvasData = {};
  canvasData[ars] = {};
  canvasData[brl] = {};

  Object.keys(canvasData).forEach(function(key){

    canvasData[key]['title'] = document.getElementById(`title-${key}`);

    let table = document.getElementById(`dynamic-table-${key}`);
    if (table !== null) {
      let tableCloned = table.cloneNode(true);
      tableCloned.classList.add("print-table");
      document.getElementsByClassName('wrapper')[0].appendChild(tableCloned);
      canvasData[key]['table'] = tableCloned;
    }

    var svgChart = document.getElementById(`chart-${key}`).innerHTML;
    var svgLegend = document.getElementById(`legend-${key}`).innerHTML;

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
  let canvasData = createCanvasData('ARS', 'BRL');

  let doc = new jsPDF('p', 'pt', 'a4');

  html2canvas(navBar)
  .then(canvas => {
    doc.internal.scaleFactor = 2.9;
    doc.addImage(canvas, 0, 0);
    doc.internal.scaleFactor = 3.6;
  })
  .then(canvas => html2canvas(canvasData.ARS.title))
  .then(canvas => doc.addImage(canvas, 60, 45))
  .then(canvas => html2canvas(canvasData.ARS.table))
  .then(canvas => {
    doc.addImage(canvas, 60, 85);
    doc.addImage(canvasData.ARS.imgChart, 'PNG', 80, 245, 180, 180);
    doc.addImage(canvasData.ARS.imgLegend, 'PNG', 280, 255, 180, 160);
  }).then(canvas => html2canvas(canvasData.BRL.title))
  .then(canvas => doc.addImage(canvas, 60, 440))
  .then(canvas => html2canvas(canvasData.BRL.table))
  .then(canvas => {
    doc.addImage(canvas, 60, 480);
    doc.addImage(canvasData.BRL.imgChart, 'PNG', 80, 640, 180, 180);
    doc.addImage(canvasData.BRL.imgLegend, 'PNG', 280, 650, 180, 160);
    doc.save(`${filename}_${getTimeNow()}.pdf`);
    Array.from(document.getElementsByClassName('print-table')).map(e => e.remove());
  });

};

function generateDashboardPDF() {

  let navBar = document.getElementsByTagName("nav")[0];
  let title = document.getElementById('title');

  let summarizedData = document.getElementById('summarized-data').cloneNode(true);
  summarizedData.classList.add("print-table");
  document.getElementsByClassName('wrapper')[0].appendChild(summarizedData);

  let doc = new jsPDF('p', 'pt', 'a4');

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
  .then(canvas => {
    doc.save(`dashboard_${getTimeNow()}.pdf`);
    Array.from(document.getElementsByClassName('print-table')).map(e => e.remove());
  });

};

function generateDetailPDF(filename) {

  let navBar = document.getElementsByTagName("nav")[0];
  let title = document.getElementById('title');

  let summarizedData = document.getElementById('summarized-data').cloneNode(true);
  summarizedData.classList.add("print-table");
  document.getElementsByClassName('wrapper')[0].appendChild(summarizedData);

  let doc = new jsPDF('p', 'pt', 'a4');

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
  .then(canvas => {
    doc.save(`${filename}_${getTimeNow()}.pdf`);
    Array.from(document.getElementsByClassName('print-table')).map(e => e.remove());
  });


};

function generateDashboardChartPDF() {

  let navBar = document.getElementsByTagName("nav")[0];
  let canvasData = createCanvasData('Argentina', 'Brazil');

  let doc = new jsPDF('p', 'pt', 'a4');

  html2canvas(navBar)
  .then(canvas => {
    doc.internal.scaleFactor = 2.9;
    doc.addImage(canvas, 0, 0);
    doc.internal.scaleFactor = 3.2;
  })
  .then(canvas => html2canvas(canvasData.Argentina.title))
  .then(canvas => {
    doc.addImage(canvas, 80, 60);
    doc.addImage(canvasData.Argentina.imgChart, 'PNG', 60, 80, 180, 180);
    doc.addImage(canvasData.Argentina.imgLegend, 'PNG', 60, 260, 180, 60);
  }).then(canvas => html2canvas(canvasData.Brazil.title))
  .then(canvas => {
    doc.addImage(canvas, 340, 60);
    doc.addImage(canvasData.Brazil.imgChart, 'PNG', 320, 80, 180, 180);
    doc.addImage(canvasData.Brazil.imgLegend, 'PNG', 320, 260, 180, 60);
    doc.save(`dashboard_charts_${getTimeNow()}.pdf`);
  });

}

function getTimeNow() {
  return new Date().toLocaleString('fr-CA').replace(' h ', 'h').replace(" min ", "m").replace(" s", "s").replace(" ", "_");
}
