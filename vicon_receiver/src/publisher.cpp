#include "vicon_receiver/publisher.hpp"

Publisher::Publisher(std::string topic_name, rclcpp::Node *node)
{
  position_publisher_ = node->create_publisher<nav_msgs::msg::Odometry>(topic_name, 10);
  is_ready = true;
}

void Publisher::publish(nav_msgs::msg::Odometry odom_msg)
{
  position_publisher_->publish(odom_msg);
}
