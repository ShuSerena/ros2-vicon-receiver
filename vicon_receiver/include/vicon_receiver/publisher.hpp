#ifndef PUBLISHER_HPP
#define PUBLISHER_HPP
#include "nav_msgs/msg/odometry.hpp"
#include "rclcpp/rclcpp.hpp"
#include <unistd.h>

// Class that allows segment data to be published in a ROS2 topic.
class Publisher
{
private:
  rclcpp::Publisher<nav_msgs::msg::Odometry>::SharedPtr position_publisher_;

public:
  bool is_ready = false;

  Publisher(std::string topic_name, rclcpp::Node *node);

  // Publishes the given odometry in the ROS2 topic whose name is indicated in
  // the constructor.
  void publish(nav_msgs::msg::Odometry odom);
};

#endif