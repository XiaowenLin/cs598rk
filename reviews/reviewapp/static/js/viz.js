
var margin = {top: 30, right: 10, bottom: 10, left: 10},
    width = 960 - margin.left - margin.right,
    height = 500 - margin.top - margin.bottom;

var x = d3.scale.linear()
    .range([100, width]);

var y = d3.scale.ordinal()
    .rangeRoundBands([0, height], .2);

var xAxis = d3.svg.axis()
    .scale(x)
    .orient("top");

var svg = d3.select(".visualization").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

//data = {{ data }}
d3.json( "../data/{{ product }}.json",  function(error, data) {

  x.domain([-1 * d3.max(data, function(d) { return d.neg; }), d3.max(data, function(d) { return d.pos; })]).nice();
  y.domain(data.map(function(d) { return d.feature; }));

  console.log(data);
  var features = svg.selectAll(".bar")
      .data(data);
  
  features.enter().append("rect")
        .attr("class", function(d) { return d.pos < 0 ? "bar negative" : "bar positive"; })
        .attr("x", function(d) { return x(Math.min(0, d.pos)); })
        .attr("y", function(d, i ) { return i*100; })
        .attr("width", function(d) { return Math.abs(x(d.pos) - x(0)); })
        .attr("height", y.rangeBand())


  
  features.enter().append("rect")
        .attr("class", function(d) { return "bar negative" })
        .attr("x", function(d) { return x(-1* d.neg); })
        .attr("y", function(d, i ) { return i*100; })
        .attr("width", function(d) { return Math.abs(x(d.neg) - x(0)); })
        .attr("height", y.rangeBand());

  features.enter().append("text")
    .attr("x", function(d) { return 10; })
    .attr("y", function(d, i ) { return i*100 + 50; })
    .attr("dy", ".35em")
    .text(function(d) { return d.feature; });

  svg.append("g")
      .attr("class", "x axis")
      .call(xAxis);

  svg.append("g")
      .attr("class", "y axis")
      .append("line")
        .attr("x1", x(0))
        .attr("x2", x(0))
        .attr("y2", height);
});

