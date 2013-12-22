var data = [];
var width = 700;
var height = 500;


d3.csv("select_bigs.csv", function(csv_data) {

  // make the data into an x y graph

  var xy_data = [{
    key: "C",
    values: []
  }, {
    key: "PF",
    values: []
  }]
  console.log(csv_data);

  _.each(csv_data, function(d, i) {

    var values = {
      x: +d.PER,
      y: +d.Salary,
      player: d.Player,
      size: d.WS_48
    }

    if (d.Position === "PF") {
      xy_data[1].values.push(values)
    } else {
      xy_data[0].values.push(values)
    }
  });

  nv.addGraph(function() {

    var chart = nv.models.scatterChart()
      .showDistX(true)
      .showDistY(true)
      .width(width)
      .height(height)
      .margin({
        top: 50,
        right: 50,
        bottom: 50,
        left: 100
      })
      .useVoronoi(false)
    //.interactive(false)

    chart.xAxis
      .axisLabel('PER')
      .tickFormat(d3.format('.1f'));

    chart.yAxis
      .axisLabel('Salary ($mm)')
      .tickFormat(d3.format('.1f'));

    chart.tooltipContent(function(key, x, y, point) {
      console.log("key is: ", key, x, y, point)
      return '<h2>' + point.point.player + '</h2>Win share: ' + point.point.size;
    });

    d3.select('#chart svg')
      .attr('width', width)
      .attr('height', height)
      .datum(xy_data)
      .transition().duration(500)
      .call(chart);

    return chart;


    // chart.xAxis
    //     .axisLabel('Time (ms)')
    //     .tickFormat(d3.format(',r'));

    // chart.yAxis
    //     .axisLabel('Voltage (v)')
    //     .tickFormat(d3.format('.02f'));

    return chart;
  });

});


// _.each(data, function(d,i){
// 	var modulus = i % 11;
// 	d.color = colorbrewer.RdYlGn[11][modulus]
// 	console.log(d,modulus);
// })