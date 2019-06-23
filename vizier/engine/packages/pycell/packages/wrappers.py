'''
Created on Sep 25, 2018

@author: michaelbrachmann
'''

import base64
import json

class GoogleMapClusterWrapper():
    
    def do_output(self, centerlat, centerlon, latlngjson):
        print("""
        <div style="width:100%; height:500px">
            <style>
              /* Always set the map height explicitly to define the size of the div
               * element that contains the map. */
              #map {
                height: 100%;
              }
              /* Optional: Makes the sample page fill the window. */
              html, body {
                height: 100%;
                margin: 0;
                padding: 0;
              }
            </style>
            <div id="map"></div>
            <img src onerror='{
                
                window.loadjscssfile = function(filename, filetype, async){
                    if (filetype=="js"){ //if filename is a external JavaScript file
                        var fileref=document.createElement("script")
                        fileref.setAttribute("type","text/javascript")
                        fileref.setAttribute("src", filename)
                        if(async){
                            fileref.setAttribute("async", "")
                            fileref.setAttribute("defer", "")
                        }
                    }
                    else if (filetype=="css"){ //if filename is an external CSS file
                        var fileref=document.createElement("link")
                        fileref.setAttribute("rel", "stylesheet")
                        fileref.setAttribute("type", "text/css")
                        fileref.setAttribute("href", filename)
                    }
                    if (typeof fileref!="undefined")
                        document.getElementsByTagName("head")[0].appendChild(fileref)
                }
                
                window.googMapAdded = false;
                
                window.initMap = function(){
                    var map = new google.maps.Map(document.getElementById("map"), {
                      center: {lat: """+str(centerlat)+""", lng: """+str(centerlon)+"""},
                      zoom: 8
                    });
                    var json = """+latlngjson+""";
        
                    var features = [];
        
                    for (var i = 0; i < json.length; i++) {
                      var lat = json[i]["lat"];
                      var lng = json[i]["lng"]; 
        
                      features.push({
                        position: new google.maps.LatLng(lat, lng),
                      });
                    }
        
                    var markers = features.map(function(feature, i) {
                        return new google.maps.Marker({
                            position: feature.position});
                    });
        
                    /*features.forEach(function(feature) {
                      var marker1 = new google.maps.Marker({
                        position: feature.position,
                        map: map
                      });
                    });*/
                    console.log("init map")
                    
                    window.markerCluster = new MarkerClusterer(map, markers,
                    {imagePath: "https://developers.google.com/maps/documentation/javascript/examples/markerclusterer/m"});
              
                }
                
                if(!window.googMapAdded){
                  window.loadjscssfile("https://developers.google.com/maps/documentation/javascript/examples/markerclusterer/markerclusterer.js","js", false)
                  window.loadjscssfile("https://maps.googleapis.com/maps/api/js?key=AIzaSyAKc9sTF-pVezJY8-Dkuvw07v1tdYIKGHk&callback=window.initMap", "js", true)
                }
                else {
                  window.markerCluster.clearMarkers();
                  window.initMap();
                }
            }'>
          </div>
        """)

class LeafletClusterWrapper():

    def do_output(self, addrpts, centerlat, centerlon, zoom, width, height):       
        html = """<!DOCTYPE html>
        <html>
        <head>
            <title>Leaflet debug page</title>
        
            <link rel="stylesheet" href="https://unpkg.com/leaflet@1.0.3/dist/leaflet.css" integrity="sha512-07I2e+7D8p6he1SIM+1twR5TIrhUQn9+I6yjqD53JQjFiMf8EtC93ty0/5vJTZGF8aAocvHYNEDJajGdNx1IsQ==" crossorigin="" />
            <script src="https://unpkg.com/leaflet@1.0.3/dist/leaflet-src.js" integrity="sha512-WXoSHqw/t26DszhdMhOXOkI7qCiv5QWXhH9R7CgvgZMHz1ImlkVQ3uNsiQKu5wwbbxtPzFXd1hK4tzno2VqhpA==" crossorigin=""></script>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
            #map {
                width: """+width+"""; 
                height: """+str(int(height)-30)+"""px; 
                border: 1px solid #ccc;
            }
        
            #progress {
                display: none;
                position: absolute;
                z-index: 1000;
                left: 400px;
                top: 300px;
                width: 200px;
                height: 20px;
                margin-top: -20px;
                margin-left: -100px;
                background-color: #fff;
                background-color: rgba(255, 255, 255, 0.7);
                border-radius: 4px;
                padding: 2px;
            }
        
            #progress-bar {
                width: 0;
                height: 100%;
                background-color: #76A6FC;
                border-radius: 4px;
            }
            </style>
        
            <link rel="stylesheet" href="https://leaflet.github.io/Leaflet.markercluster/dist/MarkerCluster.css" />
            <link rel="stylesheet" href="https://leaflet.github.io/Leaflet.markercluster/dist/MarkerCluster.Default.css" />
            <script src="https://leaflet.github.io/Leaflet.markercluster/dist/leaflet.markercluster-src.js"></script>
            <script> var addressPoints = ["""+",\n".join(addrpts)+"""]</script>
        
        </head>
        <body>
        
            <div id="map"></div>
            <script type="text/javascript">
        
                var tiles = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                        maxZoom: 18
                    }),
                    latlng = L.latLng("""+centerlat+""","""+centerlon+""");
        
                var map = L.map('map', {center: latlng, zoom: """+zoom+""", layers: [tiles]});
        
                var markers = L.markerClusterGroup();
                
                for (var i = 0; i < addressPoints.length; i++) {
                    var a = addressPoints[i];
                    var title = a[2]+" @ ("+a[0]+","+a[1]+")";
                    var marker = L.marker(new L.LatLng(a[0], a[1]), { title: title });
                    marker.bindPopup(title);
                    markers.addLayer(marker);
                }
        
                map.addLayer(markers);
        
            </script>
        </body>
        </html>
        """
        
        iframe_html = '<iframe src="data:text/html;base64,{html}" width="{width}" height="{height}"></iframe>'\
         .format(html = base64.b64encode(html),
                    width = width,
                    height= int(height),
                   )
            
        print(iframe_html)
        
        
    
class D3ChartWrapper():
    
    def do_output(self, data, charttype, width, height, title='', subtitle='', legendtitle='', xlabel='', ylabel=''):
        jsData =  json.dumps(data)
        html = """<!DOCTYPE html>
            <html>
            
            <head>
              <title>d3-ez : Showcase</title>
              <script src="https://s3.amazonaws.com/vizier-tmp/html2canvas.min.js"></script>
              <script src="https://s3.amazonaws.com/vizier-tmp/jspdf.min.js"></script>
              <script src="https://d3js.org/d3.v5.min.js"></script>
              <script src="https://s3.amazonaws.com/vizier-tmp/d3-ez.min.js"></script>
              <link rel="stylesheet" type="text/css" href="https://s3.amazonaws.com/vizier-tmp/d3-ez.css" />
            </head>
            
            <body>
              <div style="text-align:center;" id="chartholder"></div>
            
              <script type="text/javascript">
                var width = """+width+""";
                var height = """+height+""";
                var charttype = '"""+charttype+"""';
                var xlabel = '"""+xlabel+"""';
                var ylabel = '"""+ylabel+"""';
                var ctitle = '"""+title+"""';
                var csubtitle = '"""+subtitle+"""';
                var legendtitle = '"""+legendtitle+"""';
                
                function randomNum() {
                  return Math.floor(Math.random() * 10);
                };
                
                function downloadPDF() {
                    var input = document.getElementById('chartholder');
                    html2canvas(input)
                      .then((canvas) => {
                        var imgData = canvas.toDataURL('image/png');
                        var dwnldName = ctitle == '' || ctitle == 'None' ? charttype : ctitle;
                        var pdf = new jsPDF({
                          orientation: 'landscape',
                          unit: 'in'
                        });
                        pdf.addImage(imgData, 'JPEG', 0, 0);
                        pdf.save(dwnldName+".pdf");
                      })
                    ;
                }
                
                // Generate some sample data
                var data = """+jsData+""";
                //console.log(data);
                // Setup chart components
                var chartTypes = {};
                chartTypes.table = function(){
                    return d3.ez.component.htmlTable()
                      .width(width) 
                      .on("customSeriesClick", function(d) {
                        update(d,'bar');
                      });
                  }
                chartTypes.bar = function(){
                    return d3.ez.chart.barChartVertical()
                      .on("customValueMouseOver", function(d) {
                        console.log(d);
                      });
                  }
                chartTypes.bar_stacked = function(){
                    return d3.ez.chart.barChartStacked();
                  }
                chartTypes.bar_horizontal = function(){
                    return d3.ez.chart.barChartHorizontal();
                  }
                chartTypes.bar_circular = function(){
                    return d3.ez.chart.barChartCircular();
                  }
                chartTypes.bar_cluster = function(){
                    return d3.ez.chart.barChartClustered()
                      .on("customSeriesClick", function(d) {
                        update(d, 'bar');
                      });
                  }
                chartTypes.donut = function(){
                    return d3.ez.chart.donutChart()
                      .innerRadius(40);
                  }
                chartTypes.polar = function(){
                    return d3.ez.chart.polarAreaChart();
                  }
                
                chartTypes.heat_rad = function(){
                    return d3.ez.chart.heatMapRadial()
                      .innerRadius(5);
                  }
                chartTypes.heat_table = function(){
                    return d3.ez.chart.heatMapTable();
                  }
                chartTypes.punch = function(){
                    return d3.ez.chart.punchCard()
                      .minRadius(2)
                      .useGlobalScale(true)
                      .maxRadius(18)
                      .on("customValueMouseOver", function(d) {
                        console.log(d);
                      });
                  }
                chartTypes.bubble = function(){
                    return d3.ez.chart.bubbleChart();
                  }
                chartTypes.candle = function(){
                    return d3.ez.chart.candlestickChart();
                  }
                chartTypes.line = function(){
                    return d3.ez.chart.lineChart();
                  }
                chartTypes.radar = function(){
                    return d3.ez.chart.radarChart();
                  }
                chartTypes.rose = function(){
                    return d3.ez.chart.roseChart();
                  }
                // Functions to add charts to page and update charts
                //console.log(Object.keys(chartTypes));
                function update(data,charttype) {
                    var chartObj = charttype == 'table' ? 
                        chartTypes[charttype]() : d3.ez.base()
                            .width(width)
                            .height(height)
                            .chart(chartTypes[charttype]())
                    if(xlabel != '' && xlabel != 'None'){
                      chartObj.xAxisLabel(xlabel);
                    }
                    if(ylabel != '' && ylabel != 'None'){
                      chartObj.yAxisLabel(ylabel);
                    }
                    if(legendtitle != '' && legendtitle != 'None'){
                        var legend = d3.ez.component.legend()
                            .title(legendtitle);
                        chartObj.legend(legend)
                    }
                    if(ctitle != '' && ctitle != 'None'){
                        var title = d3.ez.component.title()
                            .mainText(ctitle)
                            .subText(csubtitle);
                        chartObj.title(title)
                    }
                    d3.select("#chartholder")
                      .datum(data)
                      .call(chartObj);
                    var elem = document.getElementById('creditTag');
                    if(elem)
                        elem.parentNode.removeChild(elem);
                    
      
                }
                update(data, charttype); 
              </script>
              <button style="float:right; margin-left: 5px; color: #fff; background-color: #5cb85c; border-color: #4cae4c; display: inline-block; margin-bottom: 0; font-weight: 400; text-align: center; vertical-align: middle; -ms-touch-action: manipulation; touch-action: manipulation; cursor: pointer; background-image: none; border: 1px solid transparent;     white-space: nowrap; padding: 6px 12px; font-size: 14px; line-height: 1.42857143; border-radius: 4px; -webkit-user-select: none;     -moz-user-select: none; -ms-user-select: none; user-select: none;" onclick="downloadPDF()"><i aria-hidden="true" class="download icon">
              </i> Download PDF</button>
            </body>
            
            </html>""" 
            
        iframe_html = '<iframe src="data:text/html;base64,{html}" width="{width}" height="{height}"></iframe>'\
         .format(html = base64.b64encode(html),
                    width = '100%',
                    height= int(height)+60,
                   )
            
        print(iframe_html)
