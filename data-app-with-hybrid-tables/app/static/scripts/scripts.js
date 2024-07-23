function polarToCartesian(centerX, centerY, radius, angleInDegrees) {
    var angleInRadians = (angleInDegrees-90) * Math.PI / 180.0;

    return {
      x: centerX + (radius * Math.cos(angleInRadians)),
      y: centerY + (radius * Math.sin(angleInRadians))
    };
  }

  function describeArc(x, y, radius, startAngle, endAngle){

      var start = polarToCartesian(x, y, radius, endAngle);
      var end = polarToCartesian(x, y, radius, startAngle);

      var largeArcFlag = endAngle - startAngle <= 180 ? "0" : "1";

      var d = [
          "M", start.x, start.y, 
          "A", radius, radius, 0, largeArcFlag, 0, end.x, end.y
      ].join(" ");

      return d;       
  }

  function getPiePath(percent){
    startAngle = 0
    endAngle = percent * 360
    path = describeArc(0.5, 0.5, 0.5, startAngle, endAngle) + ' L 0.5 0.5 z';
    return path;
  }

  function renderPieChart(elem){
    pathElem = elem.querySelector('g path')
    percent = pathElem.getAttribute('chart-pie-percent')
    pathElem.setAttribute('d', getPiePath(1-percent))
    textElem = elem.querySelector('text')
    textElem.innerHTML = Math.round(percent * 100) + '<tspan font-size="0.2" dy="-0.07">%</tspan>'
  }

  document.querySelectorAll('.pie-chart').forEach(el => renderPieChart(el))

  function toggleLabelToTask(label){
    labels_elem = document.querySelector('#task_labels');
    labels = labels_elem.value.split(',').map(x => x.trim()).filter(x => x !== '');
    label_chip_elem = document.querySelector('#task_labels_' + label)
  
    if(labels.includes(label)){
      labels = labels.filter(x => x !== label);
      label_chip_elem.classList.remove('label-enabled');
      label_chip_elem.classList.add('label-disabled');
    }
    else {
      labels.push(label);
      label_chip_elem.classList.add('label-enabled');
      label_chip_elem.classList.remove('label-disabled');
    }
    labels_elem.value = labels.join(', ');
  }
