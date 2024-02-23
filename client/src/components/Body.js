import React, { useState } from "react";
import axios from "axios";

const Body = () => {
  const [imageUrls, setImageUrls] = useState([]);
  const [showSuccessMessage, setShowSuccessMessage] = useState(false);

  const fetchImageUrls = async () => {
    try {
      const response = await axios.get("http://localhost:3002/album");
      setImageUrls(response.data);
    } catch (error) {
      console.error("Error fetching image URLs:", error);
    }
  };

  const handleUpload = async () => {
    try {
      const formData = new FormData();
      const fileInput = document.getElementById("fileInput");
      const files = fileInput.files;

      for (let i = 0; i < files.length; i++) {
        formData.append("images", files[i]);
      }

      const response = await fetch("http://localhost:3002/uploads", {
        method: "POST",
        body: formData,
      });

      if (response.ok) {
        const data = await response.json();
        console.log(data); // Output server response
        setShowSuccessMessage(true); // Show success message
        setTimeout(() => {
          setShowSuccessMessage(false); // Hide success message after 3 seconds
        }, 3000);
      } else {
        console.error("Failed to upload images");
      }
    } catch (error) {
      console.error("Error occurred while uploading images:", error);
    }
  };

  return (
    <div style={{ textAlign: "center", marginTop: "20px" }}>
      <div style={{ fontSize: "24px", marginBottom: "20px" }}>Body</div>

      <input
        type="file"
        id="fileInput"
        style={{ position: "absolute", left: "-9999px" }}
        multiple
        accept="image/*"
        onChange={handleUpload}
      />
      <button
        style={{
          backgroundColor: "#007bff",
          color: "#fff",
          border: "none",
          padding: "10px 20px",
          marginRight: "10px",
          borderRadius: "5px",
          cursor: "pointer",
          fontSize: "16px",
          transition: "background-color 0.3s ease",
        }}
        onClick={() => document.getElementById("fileInput").click()}
      >
        Upload Images
      </button>

      <button 
        className="button" 
        onClick={fetchImageUrls} 
        style={{
          backgroundColor: "#007bff",
          color: "#fff",
          border: "none",
          padding: "10px 20px",
          marginRight: "10px",
          borderRadius: "5px",
          cursor: "pointer",
          fontSize: "16px",
          transition: "background-color 0.3s ease",
        }}
      >
        View Images
      </button>

      <br></br>

      {showSuccessMessage && (
        <div style={{ backgroundColor: "#4CAF50", color: "white", padding: "10px", marginTop: "10px" }}>
          Image uploaded successfully!
        </div>
      )}

      <div style={{ marginTop: "20px" }}>
        {imageUrls.length === 0 ? (
          <div>No images to display</div>
        ) : (
          imageUrls.map((imageUrl, index) => (
            <img key={index} src={imageUrl} style={{ width: "450px", height: "330px", margin: "5px" }} />
          ))
        )}
      </div>

    </div>
  );
};

export default Body;
