package edu.hawaii.ics621;

import java.util.Collection;

public class DoublePoint {
  private double x;
  private double y;
  
  public DoublePoint(double x, double y) {
    this.x = x;
    this.y = y;
  }
  
  public double getX() {
    return this.x;
  }
  
  public double getY() {
    return this.y;
  }
  
  public double distance(DoublePoint point) {
    double dx = this.getX() - point.getX();
    dx *= dx;
    
    double dy = this.getY() - point.getY();
    dy *= dy;
    
    return Math.sqrt(dx + dy);
  }
  
  public static DoublePoint average(Collection<DoublePoint> points) {
    double x = 0;
    double y = 0;
    for (DoublePoint point : points) {
      x += point.getX();
      y += point.getY();
    }
    
    return new DoublePoint(x / points.size(), y / points.size());
  }
  
  public String toString() {
    return "(" + this.x + "," + this.y + ")";
  }
}
